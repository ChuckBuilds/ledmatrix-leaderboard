"""
Image Renderer for Leaderboard Plugin

Handles all image creation and rendering for the scrolling leaderboard display.
Includes logo loading, text drawing with outlines, and layout calculations.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from PIL import Image, ImageDraw, ImageFont

# Try to import logo downloader
try:
    from src.logo_downloader import download_missing_logo
except ImportError:
    # Fallback - plugins may have their own logo downloader
    try:
        import sys
        # Look for logo downloader in parent directories
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
        from src.logo_downloader import download_missing_logo
    except ImportError:
        def download_missing_logo(*args, **kwargs):
            return False


class ImageRenderer:
    """Handles image creation and rendering for leaderboard display."""
    
    def __init__(self, display_height: int, logger: Optional[logging.Logger] = None):
        """
        Initialize image renderer.
        
        Args:
            display_height: Height of the display in pixels
            logger: Optional logger instance
        """
        self.display_height = display_height
        self.logger = logger or logging.getLogger(__name__)
        self.fonts = self._load_fonts()
    
    def _load_fonts(self) -> Dict[str, ImageFont.FreeTypeFont]:
        """Load fonts for the leaderboard display."""
        fonts = {}
        try:
            # Try to load the Press Start 2P font first
            fonts['small'] = ImageFont.truetype("assets/fonts/PressStart2P-Regular.ttf", 6)
            fonts['medium'] = ImageFont.truetype("assets/fonts/PressStart2P-Regular.ttf", 10)
            fonts['large'] = ImageFont.truetype("assets/fonts/PressStart2P-Regular.ttf", 12)
            fonts['xlarge'] = ImageFont.truetype("assets/fonts/PressStart2P-Regular.ttf", 14)
            self.logger.info("Successfully loaded Press Start 2P font")
        except IOError:
            self.logger.warning("Press Start 2P font not found, trying 4x6 font")
            try:
                fonts['small'] = ImageFont.truetype("assets/fonts/4x6-font.ttf", 6)
                fonts['medium'] = ImageFont.truetype("assets/fonts/4x6-font.ttf", 8)
                fonts['large'] = ImageFont.truetype("assets/fonts/4x6-font.ttf", 10)
                fonts['xlarge'] = ImageFont.truetype("assets/fonts/4x6-font.ttf", 12)
                self.logger.info("Successfully loaded 4x6 font")
            except IOError:
                self.logger.warning("4x6 font not found, using default PIL font")
                default_font = ImageFont.load_default()
                fonts = {
                    'small': default_font,
                    'medium': default_font,
                    'large': default_font,
                    'xlarge': default_font
                }
        except Exception as e:
            self.logger.error(f"Error loading fonts: {e}")
            default_font = ImageFont.load_default()
            fonts = {
                'small': default_font,
                'medium': default_font,
                'large': default_font,
                'xlarge': default_font
            }
        return fonts
    
    def _draw_text_with_outline(self, draw: ImageDraw.Draw, text: str, position: tuple, 
                                font: ImageFont.FreeTypeFont, fill: tuple = (255, 255, 255), 
                                outline_color: tuple = (0, 0, 0)):
        """Draw text with a black outline for better readability on LED matrix."""
        x, y = position
        # Draw outline
        for dx, dy in [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]:
            draw.text((x + dx, y + dy), text, font=font, fill=outline_color)
        # Draw text
        draw.text((x, y), text, font=font, fill=fill)
    
    def _get_team_logo(self, league: str, team_id: str, team_abbr: str, logo_dir: str) -> Optional[Image.Image]:
        """Get team logo from the configured directory, downloading if missing."""
        if not team_abbr or not logo_dir:
            self.logger.debug("Cannot get team logo with missing team_abbr or logo_dir")
            return None
        try:
            logo_path = Path(logo_dir, f"{team_abbr}.png")
            if os.path.exists(logo_path):
                logo = Image.open(logo_path)
                self.logger.debug(f"Successfully loaded logo for {team_abbr}")
                return logo
            else:
                self.logger.warning(f"Logo not found at path: {logo_path}")
                
                # Try to download the missing logo
                if league:
                    self.logger.info(f"Attempting to download missing logo for {team_abbr} in league {league}")
                    success = download_missing_logo(league, team_id, team_abbr, logo_path, None)
                    if success and os.path.exists(logo_path):
                        logo = Image.open(logo_path)
                        self.logger.info(f"Successfully downloaded and loaded logo for {team_abbr}")
                        return logo
                
                return None
        except Exception as e:
            self.logger.error(f"Error loading logo for {team_abbr}: {e}")
            return None
    
    def _get_league_logo(self, league_logo_path: str) -> Optional[Image.Image]:
        """Get league logo from the configured path."""
        if not league_logo_path:
            return None
        try:
            if os.path.exists(league_logo_path):
                logo = Image.open(league_logo_path)
                self.logger.debug(f"Successfully loaded league logo from {league_logo_path}")
                return logo
            else:
                self.logger.warning(f"League logo not found at path: {league_logo_path}")
                return None
        except Exception as e:
            self.logger.error(f"Error loading league logo: {e}")
            return None
    
    def create_leaderboard_image(self, leaderboard_data: List[Dict[str, Any]]) -> Optional[Image.Image]:
        """
        Create the scrolling leaderboard image.
        
        Args:
            leaderboard_data: List of league data dictionaries with teams
            
        Returns:
            PIL Image containing the full scrolling leaderboard, or None on error
        """
        if not leaderboard_data:
            self.logger.warning("No leaderboard data available")
            return None
        
        try:
            height = self.display_height
            spacing = 40  # Spacing between leagues
            
            # Calculate total width needed
            total_width = 0
            for league_data in leaderboard_data:
                league_key = league_data['league']
                league_config = league_data['league_config']
                teams = league_data['teams']
                
                league_logo_width = 64
                teams_width = 0
                logo_size = int(height * 1.2)
                
                for i, team in enumerate(teams):
                    number_text = self._get_number_text(league_key, league_config, team, i)
                    number_bbox = self.fonts['xlarge'].getbbox(number_text)
                    number_width = number_bbox[2] - number_bbox[0]
                    
                    team_text = team['abbreviation']
                    text_bbox = self.fonts['large'].getbbox(team_text)
                    text_width = text_bbox[2] - text_bbox[0]
                    
                    team_width = number_width + 4 + logo_size + 4 + text_width + 12
                    teams_width += team_width
                
                league_width = league_logo_width + teams_width + 20
                total_width += league_width + spacing
            
            # Create the main image
            leaderboard_image = Image.new('RGB', (total_width, height), (0, 0, 0))
            draw = ImageDraw.Draw(leaderboard_image)
            
            current_x = 0
            for league_idx, league_data in enumerate(leaderboard_data):
                league_key = league_data['league']
                league_config = league_data['league_config']
                teams = league_data['teams']
                
                self.logger.info(f"Drawing League {league_idx+1} ({league_key}) starting at x={current_x}px")
                
                # Draw league logo
                league_logo = self._get_league_logo(league_config['league_logo'])
                if league_logo:
                    logo_height = height - 4
                    logo_width = int(logo_height * league_logo.width / league_logo.height)
                    logo_x = current_x + (64 - logo_width) // 2
                    logo_y = 2
                    league_logo = league_logo.resize((logo_width, logo_height), Image.Resampling.LANCZOS)
                    leaderboard_image.paste(league_logo, (logo_x, logo_y), 
                                           league_logo if league_logo.mode == 'RGBA' else None)
                
                # Move to team section
                current_x += 64 + 10
                team_x = current_x
                logo_size = int(height * 1.2)
                
                # Draw teams
                for i, team in enumerate(teams):
                    number_text = self._get_number_text(league_key, league_config, team, i)
                    number_bbox = self.fonts['xlarge'].getbbox(number_text)
                    number_width = number_bbox[2] - number_bbox[0]
                    number_height = number_bbox[3] - number_bbox[1]
                    number_y = (height - number_height) // 2
                    self._draw_text_with_outline(draw, number_text, (team_x, number_y), 
                                                self.fonts['xlarge'], fill=(255, 255, 0))
                    
                    # Draw team logo
                    team_logo = self._get_team_logo(league_key, team.get('id'), 
                                                   team['abbreviation'], league_config['logo_dir'])
                    if team_logo:
                        team_logo = team_logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)
                        logo_x = team_x + number_width + 4
                        logo_y_pos = (height - logo_size) // 2
                        leaderboard_image.paste(team_logo, (logo_x, logo_y_pos), 
                                               team_logo if team_logo.mode == 'RGBA' else None)
                        
                        # Draw team abbreviation
                        team_text = team['abbreviation']
                        text_bbox = self.fonts['large'].getbbox(team_text)
                        text_width = text_bbox[2] - text_bbox[0]
                        text_height = text_bbox[3] - text_bbox[1]
                        text_x = logo_x + logo_size + 4
                        text_y = (height - text_height) // 2
                        self._draw_text_with_outline(draw, team_text, (text_x, text_y), 
                                                    self.fonts['large'], fill=(255, 255, 255))
                        
                        team_width = number_width + 4 + logo_size + 4 + text_width + 12
                    else:
                        # Fallback if no logo
                        team_text = team['abbreviation']
                        text_bbox = self.fonts['large'].getbbox(team_text)
                        text_width = text_bbox[2] - text_bbox[0]
                        text_height = text_bbox[3] - text_bbox[1]
                        text_x = team_x + number_width + 4
                        text_y = (height - text_height) // 2
                        self._draw_text_with_outline(draw, team_text, (text_x, text_y), 
                                                    self.fonts['large'], fill=(255, 255, 255))
                        team_width = number_width + 4 + text_width + 12
                    
                    team_x += team_width
                
                current_x = team_x + 20 + spacing
            
            # Calculate actual content width
            actual_content_width = current_x - (20 + spacing)
            
            self.logger.info(f"Created leaderboard image: {total_width}px wide (actual: {actual_content_width}px)")
            return leaderboard_image
            
        except Exception as e:
            self.logger.error(f"Error creating leaderboard image: {e}")
            return None
    
    def _get_number_text(self, league_key: str, league_config: Dict[str, Any], 
                         team: Dict[str, Any], index: int) -> str:
        """Get the number/ranking text to display for a team."""
        if league_key == 'ncaa_fb':
            if league_config.get('show_ranking', True):
                if 'rank' in team and team['rank'] > 0:
                    return f"#{team['rank']}"
                else:
                    return f"{index+1}."
            else:
                if 'record_summary' in team:
                    return team['record_summary']
                else:
                    return f"{index+1}."
        else:
            return f"{index+1}."

