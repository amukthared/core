"""Class to hold all light accessories."""
from __future__ import annotations

import logging

from pyhap.const import CATEGORY_LIGHTBULB

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_BRIGHTNESS_PCT,
    ATTR_COLOR_MODE,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_HS_COLOR,
    ATTR_MAX_COLOR_TEMP_KELVIN,
    ATTR_MIN_COLOR_TEMP_KELVIN,
    ATTR_RGBW_COLOR,
    ATTR_RGBWW_COLOR,
    ATTR_SUPPORTED_COLOR_MODES,
    ATTR_WHITE,
    DOMAIN,
    ColorMode,
    brightness_supported,
    color_supported,
    color_temp_supported,
)
from homeassistant.const import (
    ATTR_ENTITY_ID,
    SERVICE_TURN_OFF,
    SERVICE_TURN_ON,
    STATE_ON,
)
from homeassistant.core import callback
from homeassistant.helpers.event import async_call_later
from homeassistant.util.color import (
    color_temperature_kelvin_to_mired,
    color_temperature_mired_to_kelvin,
    color_temperature_to_hs,
    color_temperature_to_rgbww,
)

from .accessories import TYPES, HomeAccessory
from .const import (
    CHAR_BRIGHTNESS,
    CHAR_COLOR_TEMPERATURE,
    CHAR_HUE,
    CHAR_ON,
    CHAR_SATURATION,
    PROP_MAX_VALUE,
    PROP_MIN_VALUE,
    SERV_LIGHTBULB,
)

_LOGGER = logging.getLogger(__name__)


CHANGE_COALESCE_TIME_WINDOW = 0.01

DEFAULT_MIN_COLOR_TEMP = 2000  # 500 mireds
DEFAULT_MAX_COLOR_TEMP = 6500  # 153 mireds

COLOR_MODES_WITH_WHITES = {ColorMode.RGBW, ColorMode.RGBWW, ColorMode.WHITE}


@TYPES.register("Light")
class Light(HomeAccessory):
    """Generate a Light accessory for a light entity.

    Currently supports: state, brightness, color temperature, rgb_color.
    """

    def __init__(self, *args):
        """Initialize a new Light accessory object."""
        super().__init__(*args, category=CATEGORY_LIGHTBULB)

        self.chars = []
        self._event_timer = None
        self._pending_events = {}

        state = self.hass.states.get(self.entity_id)
        attributes = state.attributes
        self.color_modes = color_modes = (
            attributes.get(ATTR_SUPPORTED_COLOR_MODES) or []
        )
        self._previous_color_mode = attributes.get(ATTR_COLOR_MODE)
        self.color_supported = color_supported(color_modes)
        self.color_temp_supported = color_temp_supported(color_modes)
        self.rgbw_supported = ColorMode.RGBW in color_modes
        self.rgbww_supported = ColorMode.RGBWW in color_modes
        self.white_supported = ColorMode.WHITE in color_modes
        self.brightness_supported = brightness_supported(color_modes)

        if self.brightness_supported:
            self.chars.append(CHAR_BRIGHTNESS)

        if self.color_supported:
            self.chars.extend([CHAR_HUE, CHAR_SATURATION])

        if self.color_temp_supported or COLOR_MODES_WITH_WHITES.intersection(
            self.color_modes
        ):
            self.chars.append(CHAR_COLOR_TEMPERATURE)

        serv_light = self.add_preload_service(SERV_LIGHTBULB, self.chars)
        self.char_on = serv_light.configure_char(CHAR_ON, value=0)

        if self.brightness_supported:
            # Initial value is set to 100 because 0 is a special value (off). 100 is
            # an arbitrary non-zero value. It is updated immediately by async_update_state
            # to set to the correct initial value.
            self.char_brightness = serv_light.configure_char(CHAR_BRIGHTNESS, value=100)

        if CHAR_COLOR_TEMPERATURE in self.chars:
            self.min_mireds = color_temperature_kelvin_to_mired(
                attributes.get(ATTR_MAX_COLOR_TEMP_KELVIN, DEFAULT_MAX_COLOR_TEMP)
            )
            self.max_mireds = color_temperature_kelvin_to_mired(
                attributes.get(ATTR_MIN_COLOR_TEMP_KELVIN, DEFAULT_MIN_COLOR_TEMP)
            )
            if not self.color_temp_supported and not self.rgbww_supported:
                self.max_mireds = self.min_mireds
            self.char_color_temp = serv_light.configure_char(
                CHAR_COLOR_TEMPERATURE,
                value=self.min_mireds,
                properties={
                    PROP_MIN_VALUE: self.min_mireds,
                    PROP_MAX_VALUE: self.max_mireds,
                },
            )

        if self.color_supported:
            self.char_hue = serv_light.configure_char(CHAR_HUE, value=0)
            self.char_saturation = serv_light.configure_char(CHAR_SATURATION, value=75)

        self.async_update_state(state)
        serv_light.setter_callback = self._set_chars

    def _set_chars(self, char_values):
        _LOGGER.debug("Light _set_chars: %s", char_values)
        # Newest change always wins
        if CHAR_COLOR_TEMPERATURE in self._pending_events and (
            CHAR_SATURATION in char_values or CHAR_HUE in char_values
        ):
            del self._pending_events[CHAR_COLOR_TEMPERATURE]
        for char in (CHAR_HUE, CHAR_SATURATION):
            if char in self._pending_events and CHAR_COLOR_TEMPERATURE in char_values:
                del self._pending_events[char]

        self._pending_events.update(char_values)
        if self._event_timer:
            self._event_timer()
        self._event_timer = async_call_later(
            self.hass, CHANGE_COALESCE_TIME_WINDOW, self._async_send_events
        )

    @callback
    def _handle_turn_on_off(self, char_values, events):
        service = SERVICE_TURN_ON
        if CHAR_ON in char_values:
            if not char_values[CHAR_ON]:
                service = SERVICE_TURN_OFF
            events.append(f"Set state to {char_values[CHAR_ON]}")
        return service

    def _handle_brightness(self, char_values, events):
        brightness_pct = None
        if CHAR_BRIGHTNESS in char_values:
            if char_values[CHAR_BRIGHTNESS] == 0:
                events[-1] = "Set state to 0"
            else:
                brightness_pct = char_values[CHAR_BRIGHTNESS]
            events.append(f"brightness at {char_values[CHAR_BRIGHTNESS]}%")
        return brightness_pct

    def _handle_white_channels(self, char_values, brightness_pct, events, params):
        if CHAR_COLOR_TEMPERATURE in char_values:
            temp = char_values[CHAR_COLOR_TEMPERATURE]
            events.append(f"color temperature at {temp}")
            bright_val = round(
                ((brightness_pct or self.char_brightness.value) * 255) / 100
            )
            # ... rest of the logic

    def _handle_color(self, char_values, events, params):
        if CHAR_HUE in char_values or CHAR_SATURATION in char_values:
            hue_sat = (
                char_values.get(CHAR_HUE, self.char_hue.value),
                char_values.get(CHAR_SATURATION, self.char_saturation.value),
            )
            events.append(f"set color at {hue_sat}")
            params[ATTR_HS_COLOR] = hue_sat

    async def async_send_events(self, *):
        """Process all changes at once."""
        _LOGGER.debug("Coalesced _set_chars: %s", self._pending_events)
        char_values = self._pending_events
        self._pending_events = {}
        events = []
        params = {ATTR_ENTITY_ID: self.entity_id}

        service = self._handle_turn_on_off(char_values, events)
        brightness_pct = self._handle_brightness(char_values, events)

        if service == SERVICE_TURN_OFF:
            self.async_call_service(
                DOMAIN, service, {ATTR_ENTITY_ID: self.entity_id}, ", ".join(events)
            )
            return

        self._handle_white_channels(char_values, brightness_pct, events, params)
        self._handle_color(char_values, events, params)

        if (
            brightness_pct
            and ATTR_RGBWW_COLOR not in params
            and ATTR_RGBW_COLOR not in params
        ):
            params[ATTR_BRIGHTNESS_PCT] = brightness_pct

        _LOGGER.debug(
            "Calling light service with params: %s -> %s", char_values, params
        )
        self.async_call_service(DOMAIN, service, params, ", ".join(events))

    @callback
   async def async_update_brightness(self, attributes, state):
    brightness = attributes.get(ATTR_BRIGHTNESS)
    if brightness is not None and isinstance(brightness, (int, float)):
        brightness = round(brightness / 255 * 100, 0)
        if brightness == 0 and state == STATE_ON:
            brightness = 1
        self.char_brightness.set_value(brightness)
        if self._previous_color_mode != attributes.get(ATTR_COLOR_MODE):
            self.char_brightness.notify()

async def async_update_color(self, attributes):
    if color_temp := attributes.get(ATTR_COLOR_TEMP_KELVIN):
        hue, saturation = color_temperature_to_hs(color_temp)
    elif attributes.get(ATTR_COLOR_MODE) == ColorMode.WHITE:
        hue, saturation = 0, 0
    else:
        hue, saturation = attributes.get(ATTR_HS_COLOR, (None, None))
    if isinstance(hue, (int, float)) and isinstance(saturation, (int, float)):
        self.char_hue.set_value(round(hue, 0))
        self.char_saturation.set_value(round(saturation, 0))
        if self._previous_color_mode != attributes.get(ATTR_COLOR_MODE):
            self.char_hue.notify()
            self.char_saturation.notify()

async def async_update_color_temperature(self, attributes):
    color_temp = None
    if self.color_temp_supported:
        color_temp_kelvin = attributes.get(ATTR_COLOR_TEMP_KELVIN)
        if color_temp_kelvin is not None:
            color_temp = color_temperature_kelvin_to_mired(color_temp_kelvin)
    elif attributes.get(ATTR_COLOR_MODE) == ColorMode.WHITE:
        color_temp = self.min_mireds
    if isinstance(color_temp, (int, float)):
        self.char_color_temp.set_value(round(color_temp, 0))
        if self._previous_color_mode != attributes.get(ATTR_COLOR_MODE):
            self.char_color_temp.notify()

async def async_update_state(self, new_state):
    """Update light after state change."""
    # Handle State
    state = new_state.state
    attributes = new_state.attributes
    color_mode = attributes.get(ATTR_COLOR_MODE)
    self.char_on.set_value(int(state == STATE_ON))
    self._previous_color_mode = color_mode
    
    # Handle Brightness
    if self.brightness_supported:
        await async_update_brightness(self, attributes, state)

    # Handle Color
    if self.color_supported:
        await async_update_color(self, attributes)

    # Handle white channels
    if CHAR_COLOR_TEMPERATURE in self.chars:
        await async_update_color_temperature(self, attributes)