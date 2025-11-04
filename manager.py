"""
Leaderboard Plugin for LEDMatrix

Displays scrolling leaderboards and standings for multiple sports leagues.
Shows team rankings, records, and statistics in a scrolling ticker format.

Features:
- Multi-sport leaderboard display (NFL, NBA, MLB, NCAA, NHL)
- Conference and division filtering
- NCAA rankings vs standings
- Scrolling ticker format with dynamic duration
- Configurable scroll speed and display options
- Background data fetching

API Version: 1.0.0
"""

import time
import logging
from typing import Dict, Any, List, Optional

from PIL import Image

from src.plugin_system.base_plugin import BasePlugin
from src.common.scroll_helper import ScrollHelper

from league_config import LeagueConfig
from data_fetcher import DataFetcher
from image_renderer import ImageRenderer

logger = logging.getLogger(__name__)


class LeaderboardPlugin(BasePlugin):
    """
    Leaderboard plugin for displaying sports standings and rankings.

    Supports multiple sports leagues with configurable display options,
    conference/division filtering, and scrolling ticker format.
    """

    def __init__(self, plugin_id: str, config: Dict[str, Any],
                 display_manager, cache_manager, plugin_manager):
        """Initialize the leaderboard plugin."""
        super().__init__(plugin_id, config, display_manager, cache_manager, plugin_manager)
        
        # Get display dimensions
        self.display_width = display_manager.width
        self.display_height = display_manager.height
        
        # Configuration
        self.global_config = config.get('global', {})
        self.update_interval = self.global_config.get('update_interval', 3600)
        
        # Display settings
        self.display_duration = self.global_config.get('display_duration', 30)
        self.scroll_speed = self.global_config.get('scroll_pixels_per_second', 15.0)
        self.scroll_delay = self.global_config.get('scroll_delay', 0.01)
        self.dynamic_duration = self.global_config.get('dynamic_duration', True)
        self.min_duration = self.global_config.get('min_duration', 30)
        self.max_duration = self.global_config.get('max_duration', 300)
        self.duration_buffer = self.global_config.get('duration_buffer', 0.1)
        self.loop = self.global_config.get('loop', True)
        
        # Request timeout
        self.request_timeout = self.global_config.get('request_timeout', 30)
        
        # Initialize components
        self.league_config = LeagueConfig(config, self.logger)
        self.data_fetcher = DataFetcher(cache_manager, self.logger, self.request_timeout)
        self.image_renderer = ImageRenderer(self.display_height, self.logger)
        
        # Initialize scroll helper
        self.scroll_helper = ScrollHelper(self.display_width, self.display_height, self.logger)
        self.scroll_helper.set_scroll_speed(self.scroll_speed)
        self.scroll_helper.set_scroll_delay(self.scroll_delay)
        self.scroll_helper.set_dynamic_duration_settings(
            enabled=self.dynamic_duration,
            min_duration=self.min_duration,
            max_duration=self.max_duration,
            buffer=self.duration_buffer
        )
        
        # State
        self.leaderboard_data = []
        self.last_update = 0
        
        # Enable scrolling for high FPS
        self.enable_scrolling = True
        
        # Log enabled leagues
        enabled_leagues = self.league_config.get_enabled_leagues()
        self.logger.info("Leaderboard plugin initialized")
        self.logger.info(f"Enabled leagues: {enabled_leagues}")
        self.logger.info(f"Display dimensions: {self.display_width}x{self.display_height}")
        self.logger.info(f"Scroll speed: {self.scroll_speed} px/s")
        self.logger.info(f"Dynamic duration: {self.dynamic_duration}")
    
    def update(self) -> None:
        """Update standings data for all enabled leagues."""
        current_time = time.time()
        
        # Check if it's time to update
        if current_time - self.last_update < self.update_interval:
            return
        
        try:
            self.logger.info("Updating leaderboard data")
            self.leaderboard_data = []
            
            # Fetch standings for each enabled league
            for league_key in self.league_config.get_enabled_leagues():
                league_config = self.league_config.get_league_config(league_key)
                if not league_config:
                    continue
                
                standings = self.data_fetcher.fetch_standings(league_config)
                
                if standings:
                    self.leaderboard_data.append({
                        'league': league_key,
                        'league_config': league_config,
                        'teams': standings
                    })
            
            self.last_update = current_time
            
            # Clear scroll cache when data updates
            self.scroll_helper.clear_cache()
            
            self.logger.info(f"Updated standings data: {len(self.leaderboard_data)} leagues, "
                           f"{sum(len(d['teams']) for d in self.leaderboard_data)} total teams")
            
        except Exception as e:
            self.logger.error(f"Error updating leaderboard data: {e}")
    
    def display(self, force_clear: bool = False) -> None:
        """Display the scrolling leaderboard."""
        if not self.enabled:
            self.logger.debug("Leaderboard plugin is disabled")
            return
        
        if not self.leaderboard_data:
            self.logger.warning("No leaderboard data available. Attempting to update...")
            self.update()
            if not self.leaderboard_data:
                self.logger.warning("Still no data after update, showing fallback")
                self._display_fallback_message()
                return
        
        # Create scrolling image if needed
        if not self.scroll_helper.cached_image or force_clear:
            self.logger.info("Creating leaderboard image...")
            self._create_leaderboard_image()
            if not self.scroll_helper.cached_image:
                self.logger.error("Failed to create leaderboard image, showing fallback")
                self._display_fallback_message()
                return
            self.logger.info("Leaderboard image created successfully")
        
        if force_clear:
            self.scroll_helper.reset_scroll()
        
        # Signal scrolling state
        self.display_manager.set_scrolling_state(True)
        self.display_manager.process_deferred_updates()
        
        # Update scroll position using the scroll helper
        self.scroll_helper.update_scroll_position()
        
        # Get visible portion
        visible_portion = self.scroll_helper.get_visible_portion()
        if visible_portion:
            # Update display
            self.display_manager.image.paste(visible_portion, (0, 0))
            self.display_manager.update_display()
        
        # Log frame rate (less frequently to avoid spam)
        self.scroll_helper.log_frame_rate()
    
    def _create_leaderboard_image(self) -> None:
        """Create the scrolling leaderboard image."""
        try:
            leaderboard_image = self.image_renderer.create_leaderboard_image(self.leaderboard_data)
            
            if leaderboard_image:
                # Set up scroll helper with the image
                self.scroll_helper.cached_image = leaderboard_image
                self.scroll_helper.total_scroll_width = leaderboard_image.width
                
                # Calculate dynamic duration
                self.scroll_helper._calculate_dynamic_duration()
                
                self.logger.info(f"Created leaderboard image: {leaderboard_image.width}x{leaderboard_image.height}")
                self.logger.info(f"Dynamic duration: {self.scroll_helper.get_dynamic_duration()}s")
            else:
                self.logger.error("Failed to create leaderboard image")
                self.scroll_helper.cached_image = None
                
        except Exception as e:
            self.logger.error(f"Error creating leaderboard image: {e}")
            self.scroll_helper.cached_image = None
    
    def _display_fallback_message(self) -> None:
        """Display a fallback message when no data is available."""
        try:
            width = self.display_width
            height = self.display_height
            
            image = Image.new('RGB', (width, height), (0, 0, 0))
            from PIL import ImageDraw
            draw = ImageDraw.Draw(image)
            
            text = "No Leaderboard Data"
            # Use default font if available
            try:
                font = self.image_renderer.fonts['medium']
            except (KeyError, AttributeError):
                from PIL import ImageFont
                font = ImageFont.load_default()
            
            text_bbox = draw.textbbox((0, 0), text, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]
            
            x = (width - text_width) // 2
            y = (height - text_height) // 2
            
            draw.text((x, y), text, font=font, fill=(255, 255, 255))
            
            self.display_manager.image = image
            self.display_manager.update_display()
            
        except Exception as e:
            self.logger.error(f"Error displaying fallback message: {e}")
    
    def get_display_duration(self) -> float:
        """Get display duration from config or dynamic calculation."""
        if self.dynamic_duration and self.scroll_helper.cached_image:
            return float(self.scroll_helper.get_dynamic_duration())
        return float(self.display_duration)
    
    def get_info(self) -> Dict[str, Any]:
        """Return plugin info for web UI."""
        info = super().get_info()
        
        leagues_config = {}
        for league_key in self.league_config.get_enabled_leagues():
            league_config = self.league_config.get_league_config(league_key)
            if league_config:
                leagues_config[league_key] = {
                    'enabled': True,
                    'top_teams': league_config.get('top_teams', 10)
                }
        
        info.update({
            'total_teams': sum(len(d['teams']) for d in self.leaderboard_data),
            'enabled_leagues': self.league_config.get_enabled_leagues(),
            'last_update': self.last_update,
            'display_duration': self.get_display_duration(),
            'scroll_speed': self.scroll_speed,
            'dynamic_duration': self.dynamic_duration,
            'min_duration': self.min_duration,
            'max_duration': self.max_duration,
            'leagues_config': leagues_config,
            'scroll_info': self.scroll_helper.get_scroll_info() if self.scroll_helper else None
        })
        return info
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        self.leaderboard_data = []
        if self.scroll_helper:
            self.scroll_helper.clear_cache()
        self.logger.info("Leaderboard plugin cleaned up")
