"""
Data Fetcher for Leaderboard Plugin

Handles all ESPN API data fetching for standings and rankings.
Includes caching, error handling, and data processing.
"""

import time
import logging
import requests
from typing import Dict, Any, List, Optional

# Try to import API counter from web interface
try:
    from web_interface_v2 import increment_api_counter
except ImportError:
    # Fallback if web interface is not available
    def increment_api_counter(kind: str, count: int = 1):
        pass


class DataFetcher:
    """Handles fetching standings and rankings data from ESPN API."""
    
    def __init__(self, cache_manager, logger: Optional[logging.Logger] = None, 
                 request_timeout: int = 30):
        """
        Initialize data fetcher.
        
        Args:
            cache_manager: Cache manager instance
            logger: Optional logger instance
            request_timeout: Request timeout in seconds
        """
        self.cache_manager = cache_manager
        self.logger = logger or logging.getLogger(__name__)
        self.request_timeout = request_timeout
    
    def fetch_standings(self, league_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch standings for a specific league from ESPN API with caching.
        
        Args:
            league_config: League configuration dictionary
            
        Returns:
            List of team standings dictionaries
        """
        league_key = league_config['league']
        cache_key = f"leaderboard_{league_key}"
        
        # Try to get cached data first
        cached_data = self.cache_manager.get_cached_data_with_strategy(cache_key, 'leaderboard')
        if cached_data:
            self.logger.info(f"Using cached leaderboard data for {league_key}")
            return cached_data.get('standings', [])
        
        # Special handling for college football - use rankings endpoint
        if league_key == 'college-football':
            return self._fetch_ncaa_fb_rankings(league_config)
        
        # Special handling for mens-college-hockey - use rankings endpoint
        if league_key == 'mens-college-hockey':
            return self._fetch_ncaam_hockey_rankings(league_config)
        
        # Use standings endpoint for NFL, MLB, NHL, and NCAA Baseball
        if league_key in ['nfl', 'mlb', 'nhl', 'college-baseball']:
            return self._fetch_standings_data(league_config)
        
        # For NBA and other leagues, use teams endpoint
        return self._fetch_teams_data(league_config)
    
    def _fetch_ncaa_fb_rankings(self, league_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch NCAA Football rankings from ESPN API using the rankings endpoint."""
        league_key = league_config['league']
        cache_key = f"leaderboard_{league_key}_rankings"
        
        # Try to get cached data first
        cached_data = self.cache_manager.get_cached_data_with_strategy(cache_key, 'leaderboard')
        if cached_data:
            self.logger.info(f"Using cached rankings data for {league_key}")
            return cached_data.get('standings', [])
        
        try:
            self.logger.info(f"Fetching fresh rankings data for {league_key}")
            rankings_url = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings"
            
            response = requests.get(rankings_url, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            increment_api_counter('sports', 1)
            
            rankings_data = data.get('rankings', [])
            if not rankings_data:
                self.logger.warning("No rankings data found")
                return []
            
            # Use the first ranking (usually AP Top 25)
            first_ranking = rankings_data[0]
            ranking_name = first_ranking.get('name', 'Unknown')
            teams = first_ranking.get('ranks', [])
            
            self.logger.info(f"Using ranking: {ranking_name}, found {len(teams)} teams")
            
            standings = []
            for team_data in teams:
                team_info = team_data.get('team', {})
                team_name = team_info.get('name', 'Unknown')
                team_id = team_info.get('id')
                team_abbr = team_info.get('abbreviation', 'Unknown')
                current_rank = team_data.get('current', 0)
                record_summary = team_data.get('recordSummary', '0-0')
                
                # Parse record
                wins, losses, ties, win_percentage = self._parse_record(record_summary)
                
                standings.append({
                    'name': team_name,
                    'id': team_id,
                    'abbreviation': team_abbr,
                    'rank': current_rank,
                    'wins': wins,
                    'losses': losses,
                    'ties': ties,
                    'win_percentage': win_percentage,
                    'record_summary': record_summary,
                    'ranking_name': ranking_name
                })
            
            top_teams = standings[:league_config.get('top_teams', 25)]
            
            # Cache the results
            cache_data = {
                'standings': top_teams,
                'timestamp': time.time(),
                'league': league_key,
                'ranking_name': ranking_name
            }
            self.cache_manager.save_cache(cache_key, cache_data)
            
            self.logger.info(f"Fetched and cached {len(top_teams)} teams for {league_key}")
            return top_teams
            
        except Exception as e:
            self.logger.error(f"Error fetching rankings for {league_key}: {e}")
            return []
    
    def _fetch_ncaam_hockey_rankings(self, league_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch NCAA Men's Hockey rankings from ESPN API."""
        league_key = league_config['league']
        cache_key = f"leaderboard_{league_key}_rankings"
        
        cached_data = self.cache_manager.get_cached_data_with_strategy(cache_key, 'leaderboard')
        if cached_data:
            self.logger.info(f"Using cached rankings data for {league_key}")
            return cached_data.get('standings', [])
        
        try:
            self.logger.info(f"Fetching fresh rankings data for {league_key}")
            rankings_url = "https://site.api.espn.com/apis/site/v2/sports/hockey/mens-college-hockey/rankings"
            
            response = requests.get(rankings_url, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            increment_api_counter('sports', 1)
            
            rankings_data = data.get('rankings', [])
            if not rankings_data:
                self.logger.warning("No rankings data found")
                return []
            
            first_ranking = rankings_data[0]
            ranking_name = first_ranking.get('name', 'Unknown')
            teams = first_ranking.get('ranks', [])
            
            standings = []
            for team_data in teams:
                team_info = team_data.get('team', {})
                team_name = team_info.get('name', 'Unknown')
                team_id = team_info.get('id')
                team_abbr = team_info.get('abbreviation', 'Unknown')
                current_rank = team_data.get('current', 0)
                record_summary = team_data.get('recordSummary', '0-0')
                
                wins, losses, ties, win_percentage = self._parse_record(record_summary)
                
                standings.append({
                    'name': team_name,
                    'id': team_id,
                    'abbreviation': team_abbr,
                    'rank': current_rank,
                    'wins': wins,
                    'losses': losses,
                    'ties': ties,
                    'win_percentage': win_percentage,
                    'record_summary': record_summary,
                    'ranking_name': ranking_name
                })
            
            top_teams = standings[:league_config.get('top_teams', 25)]
            
            cache_data = {
                'standings': top_teams,
                'timestamp': time.time(),
                'league': league_key,
                'ranking_name': ranking_name
            }
            self.cache_manager.save_cache(cache_key, cache_data)
            
            self.logger.info(f"Fetched and cached {len(top_teams)} teams for {league_key}")
            return top_teams
            
        except Exception as e:
            self.logger.error(f"Error fetching rankings for {league_key}: {e}")
            return []
    
    def _fetch_standings_data(self, league_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch standings data from ESPN API using the standings endpoint."""
        league_key = league_config['league']
        cache_key = f"leaderboard_{league_key}_standings"
        
        cached_data = self.cache_manager.get_cached_data_with_strategy(cache_key, 'leaderboard')
        if cached_data:
            self.logger.info(f"Using cached standings data for {league_key}")
            return cached_data.get('standings', [])
        
        try:
            self.logger.info(f"Fetching fresh standings data for {league_key}")
            
            standings_url = league_config['standings_url']
            params = {
                'level': league_config.get('level', 1),
                'sort': league_config.get('sort', 'winpercent:desc,gamesbehind:asc')
            }
            
            # Only include season if explicitly provided - otherwise ESPN defaults to current season
            if 'season' in league_config and league_config.get('season'):
                params['season'] = league_config['season']
            
            response = requests.get(standings_url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            increment_api_counter('sports', 1)
            
            standings = []
            
            # Parse standings structure
            if 'standings' in data and 'entries' in data['standings']:
                # Direct standings
                entries = data['standings']['entries']
                for entry in entries:
                    standing = self._extract_team_standing(entry, league_key)
                    if standing:
                        standings.append(standing)
            elif 'children' in data:
                # Children structure (divisions/conferences)
                for child in data.get('children', []):
                    entries = child.get('standings', {}).get('entries', [])
                    for entry in entries:
                        standing = self._extract_team_standing(entry, league_key)
                        if standing:
                            standings.append(standing)
            else:
                self.logger.warning(f"No standings data found for {league_key}")
                return []
            
            # Sort by win percentage and limit
            standings.sort(key=lambda x: x['win_percentage'], reverse=True)
            top_teams = standings[:league_config.get('top_teams', 10)]
            
            cache_data = {
                'standings': top_teams,
                'timestamp': time.time(),
                'league': league_key,
                'level': params['level']
            }
            # Only include season in cache if it was explicitly provided
            if 'season' in params:
                cache_data['season'] = params['season']
            self.cache_manager.save_cache(cache_key, cache_data)
            
            self.logger.info(f"Fetched and cached {len(top_teams)} teams for {league_key}")
            return top_teams
            
        except Exception as e:
            self.logger.error(f"Error fetching standings for {league_key}: {e}")
            return []
    
    def _fetch_teams_data(self, league_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch team data using teams endpoint (for NBA, etc.)."""
        league_key = league_config['league']
        cache_key = f"leaderboard_{league_key}"
        
        cached_data = self.cache_manager.get_cached_data_with_strategy(cache_key, 'leaderboard')
        if cached_data:
            self.logger.info(f"Using cached leaderboard data for {league_key}")
            return cached_data.get('standings', [])
        
        try:
            self.logger.info(f"Fetching fresh leaderboard data for {league_key}")
            teams_url = league_config['teams_url']
            response = requests.get(teams_url, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            increment_api_counter('sports', 1)
            
            standings = []
            sports = data.get('sports', [])
            
            if not sports or not sports[0].get('leagues', []):
                self.logger.warning(f"No teams data found for {league_config['league']}")
                return []
            
            teams = sports[0]['leagues'][0].get('teams', [])
            
            for team_data in teams:
                team = team_data.get('team', {})
                team_abbr = team.get('abbreviation')
                team_name = team.get('name', 'Unknown')
                
                if not team_abbr:
                    continue
                
                # Fetch individual team record
                team_record = self._fetch_team_record(team_abbr, league_config)
                
                if team_record:
                    standings.append({
                        'name': team_name,
                        'abbreviation': team_abbr,
                        'id': team.get('id'),
                        'wins': team_record.get('wins', 0),
                        'losses': team_record.get('losses', 0),
                        'ties': team_record.get('ties', 0),
                        'win_percentage': team_record.get('win_percentage', 0)
                    })
            
            standings.sort(key=lambda x: x['win_percentage'], reverse=True)
            top_teams = standings[:league_config.get('top_teams', 10)]
            
            cache_data = {
                'standings': top_teams,
                'timestamp': time.time(),
                'league': league_key
            }
            self.cache_manager.save_cache(cache_key, cache_data)
            
            self.logger.info(f"Fetched and cached {len(top_teams)} teams for {league_config['league']}")
            return top_teams
            
        except Exception as e:
            self.logger.error(f"Error fetching standings for {league_config['league']}: {e}")
            return []
    
    def _extract_team_standing(self, entry: Dict, league_key: str) -> Optional[Dict[str, Any]]:
        """Extract team standing from API entry."""
        team_data = entry.get('team', {})
        stats = entry.get('stats', [])
        
        team_name = team_data.get('displayName', 'Unknown')
        team_abbr = team_data.get('abbreviation', 'Unknown')
        team_id = team_data.get('id')
        
        wins = 0
        losses = 0
        ties = 0
        win_percentage = 0.0
        games_played = 0
        
        for stat in stats:
            stat_type = stat.get('type', '')
            stat_value = stat.get('value', 0)
            
            if stat_type == 'wins':
                wins = int(stat_value)
            elif stat_type == 'losses':
                losses = int(stat_value)
            elif stat_type == 'ties':
                ties = int(stat_value)
            elif stat_type == 'winpercent':
                win_percentage = float(stat_value)
            elif stat_type == 'overtimelosses' and league_key == 'nhl':
                ties = int(stat_value)
            elif stat_type == 'gamesplayed' and league_key == 'nhl':
                games_played = float(stat_value)
        
        if league_key == 'nhl' and win_percentage == 0.0 and games_played > 0:
            win_percentage = wins / games_played
        
        if ties > 0:
            record_summary = f"{wins}-{losses}-{ties}"
        else:
            record_summary = f"{wins}-{losses}"
        
        return {
            'name': team_name,
            'id': team_id,
            'abbreviation': team_abbr,
            'wins': wins,
            'losses': losses,
            'ties': ties,
            'win_percentage': win_percentage,
            'record_summary': record_summary
        }
    
    def _fetch_team_record(self, team_abbr: str, league_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch individual team record from ESPN API with caching."""
        league = league_config['league']
        cache_key = f"team_record_{league}_{team_abbr}"
        
        cached_data = self.cache_manager.get_cached_data_with_strategy(cache_key, 'leaderboard')
        if cached_data:
            return cached_data.get('record')
        
        try:
            sport = league_config['sport']
            
            if league == 'college-football':
                url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_abbr}"
            else:
                url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams/{team_abbr}"
            
            response = requests.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            increment_api_counter('sports', 1)
            
            team_data = data.get('team', {})
            stats = team_data.get('stats', [])
            
            wins = 0
            losses = 0
            ties = 0
            
            for stat in stats:
                if stat.get('name') == 'wins':
                    wins = stat.get('value', 0)
                elif stat.get('name') == 'losses':
                    losses = stat.get('value', 0)
                elif stat.get('name') == 'ties':
                    ties = stat.get('value', 0)
            
            total_games = wins + losses + ties
            win_percentage = wins / total_games if total_games > 0 else 0
            
            record = {
                'wins': wins,
                'losses': losses,
                'ties': ties,
                'win_percentage': win_percentage
            }
            
            cache_data = {
                'record': record,
                'timestamp': time.time(),
                'team': team_abbr,
                'league': league
            }
            self.cache_manager.save_cache(cache_key, cache_data)
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error fetching record for {team_abbr} in league {league}: {e}")
            return None
    
    def _parse_record(self, record_summary: str) -> tuple:
        """Parse record string (e.g., "12-1", "8-4", "10-2-1") into components."""
        wins = 0
        losses = 0
        ties = 0
        win_percentage = 0
        
        try:
            parts = record_summary.split('-')
            if len(parts) >= 2:
                wins = int(parts[0])
                losses = int(parts[1])
                if len(parts) == 3:
                    ties = int(parts[2])
                
                total_games = wins + losses + ties
                win_percentage = wins / total_games if total_games > 0 else 0
        except (ValueError, IndexError):
            self.logger.warning(f"Could not parse record: {record_summary}")
        
        return wins, losses, ties, win_percentage

