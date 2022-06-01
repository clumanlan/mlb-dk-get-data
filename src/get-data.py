import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import statsapi
import awswrangler as wr
from datetime import datetime, timedelta
from time import sleep

# get season start and end dates ---------------------------

seasons = []
start_dates = []
end_dates = []

for i in range(2001,2023):
    season_list = statsapi.get('season', {'sportId':1, 'seasonId': i})
    season_dict = season_list['seasons'][0]
    
    seasons.append(str(i))
    start_dates.append(str(season_dict['seasonStartDate']))
    end_dates.append(str(season_dict['seasonEndDate']))
    print(i)


# get all gamepks for games that were completed----------------------
game_pk_list = []
game_type_list = []
season_list = []

for season, start_date, end_date in zip(seasons, start_dates, end_dates):
    print("Starting season:" + season)
    sched = statsapi.get("schedule", {"sportId": 1, "startDate": start_date, "endDate": end_date,"gameType": "R,F,D,L,W", "fields": "dates,date,games,gamePk, gameType"})
    
    game_list = sched['dates']

    for date_list in game_list:
        game_dicts = date_list['games'] # returns a list of game dicts binned by date
        
        for game in game_dicts:
            game_pk_list.append(game['gamePk'])
            game_type_list.append(game['gameType'])
            season_list.append(int(season))


game_pk_df = pd.DataFrame({'game_pk':game_pk_list, 'game_type':game_type_list, 'season':season_list})
rel_game_pks = game_pk_df[game_pk_df['game_type'].isin(['R', 'F', 'D', 'L', 'W'])]['game_pk'] # filter for season/playoff games


# get game details: time, place, duration  --------------------------------------------------------------------
game_df_list = []
game_pks_2001_to_2012 = game_pk_df[game_pk_df['game_type'].isin(['R', 'F', 'D', 'L', 'W']) & (game_pk_df['season'] <= 2012)]['game_pk']
game_pks_2013_to_2022 = game_pk_df[game_pk_df['game_type'].isin(['R', 'F', 'D', 'L', 'W']) & (game_pk_df['season'] >= 2013)]['game_pk']


for pk in game_pks_2001_to_2012:
    print(pk)
    
    game = statsapi.get("game", {"gamePk":pk})
    end_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d') 

    if game['gameData']['datetime']['officialDate'] > end_date: # some future rescheduled games get snuck in to rel_game_pks
        continue
    
    gamedata = game['gameData']
 
    gamedata_keys = ['pk', 'type', 'doubleHeader', 'gamedayType', 'tiebreaker', 'gameNumber', 'season']
    gamedata_dict = {key: gamedata['game'].get(key) for key in gamedata_keys}

    datetime_keys = ['dateTime', 'officialDate', 'dayNight', 'time', 'ampm']
    datetime_dict = {key: gamedata['datetime'].get(key) for key in datetime_keys}

    status_keys = ['detailedState', 'statusCode', 'codedGameState']
    status_dict = {key: gamedata['status'].get(key) for key in status_keys}

    gameinfo_keys = ['attendance', 'gameDurationMinutes']
    gameinfo_dict = {key: gamedata['gameInfo'].get(key) for key in gameinfo_keys}

    venue_dict = gamedata['venue']
    venue_dict_filtered = {}
    venue_dict_filtered['venue_id'] = venue_dict['id']
    venue_dict_filtered['venue_name'] = venue_dict['name']
    venue_dict_filtered['venue_tz'] = venue_dict['timeZone']['tz']

    fieldinfo_dict = venue_dict['fieldInfo']

    weather_dict = gamedata['weather']

    ## get home and away team info incl current record -------------------------------------------------
    gameteams_dict = gamedata['teams']
    away_dict = gameteams_dict['away']
    home_dict = gameteams_dict['home']


    away_dict_filtered = {}
    away_dict_filtered['away_id'] = away_dict['id']
    away_dict_filtered['away_name'] = away_dict['name']
    away_dict_filtered['away_leaguename'] = away_dict['league']['name']
    away_dict_filtered['away_divisionname'] = away_dict['division']['name']
    away_dict_filtered['away_gamesplayed'] = away_dict['record']['gamesPlayed']
    away_leaguerecord_dict = away_dict['record']['leagueRecord']
    away_dict_filtered['away_wins'] = away_leaguerecord_dict['wins']
    away_dict_filtered['away_losses'] = away_leaguerecord_dict['losses']
    away_dict_filtered['away_ties'] = away_leaguerecord_dict['ties']
    away_dict_filtered['away_pct'] = away_leaguerecord_dict['pct']


    home_dict_filtered = {}
    home_dict_filtered['home_id'] = home_dict['id']
    home_dict_filtered['home_name'] = home_dict['name']
    home_dict_filtered['home_leaguename'] = home_dict['league']['name']
    home_dict_filtered['home_divisionname'] = home_dict['division']['name']
    home_dict_filtered['home_gamesplayed'] = home_dict['record']['gamesPlayed']

    home_leaguerecord_dict = home_dict['record']['leagueRecord']
    home_dict_filtered['home_wins'] = home_leaguerecord_dict['wins']
    home_dict_filtered['home_losses'] = home_leaguerecord_dict['losses']
    home_dict_filtered['home_ties'] = home_leaguerecord_dict['ties']
    home_dict_filtered['home_pct'] = home_leaguerecord_dict['pct']
    
    # winner/loser pitcher data -----------------------------------------------
    game_livedata = game['liveData']

    if 'decisions' not in game_livedata: # some games are postponed but still show up on the date
        continue 
    
    if 'winner' not in game_livedata['decisions']: # there are ties if game ends early
        continue 

    winner_dict = game_livedata['decisions']['winner']
    loser_dict = game_livedata['decisions']['loser']

    pitcher_dict = {}
    pitcher_dict['pitcher_winner_playerid'] = winner_dict['id']
    pitcher_dict['pitcher_winner_playername'] = winner_dict['fullName']
    pitcher_dict['pitcher_loser_playerid'] = loser_dict['id']
    pitcher_dict['pitcher_loser_playername'] = loser_dict['fullName']
    
    # get final line score pitching and batting (runs, hits, errors, left on base) data ------------------------------------
    boxscore = game_livedata['boxscore']['teams']

    # get pitching linescore (summary of pitching for the game) ----------------------------------
    away_pitching_dict = boxscore['away']['teamStats']['pitching']
    away_pitching_oldkeys = list(away_pitching_dict.keys())
    away_pitching_newkeys = ['away_' + s for s in away_pitching_oldkeys]
    away_pitching_vals = list(away_pitching_dict.values())
    away_pitching_dict = {k: v for k, v in zip(away_pitching_newkeys, away_pitching_vals)}

    home_pitching_dict = boxscore['home']['teamStats']['pitching']
    home_pitching_oldkeys = list(home_pitching_dict.keys())
    home_pitching_newkeys = ['away_' + s for s in home_pitching_oldkeys]
    home_pitching_vals = list(home_pitching_dict.values())
    home_pitching_dict = {k: v for k, v in zip(home_pitching_newkeys, home_pitching_vals)}

    # get batting linescore (summary of batting for the game) ------------------------------------

    away_batting_dict = boxscore['away']['teamStats']['batting']
    away_batting_oldkeys = list(away_batting_dict.keys())
    away_batting_newkeys = ['away_' + s for s in away_batting_oldkeys]
    away_batting_vals = list(away_batting_dict.values())
    away_batting_dict = {k: v for k, v in zip(away_batting_newkeys, away_batting_vals)}

    home_batting_dict = boxscore['home']['teamStats']['batting']
    home_batting_oldkeys = list(home_batting_dict.keys())
    home_batting_newkeys = ['home_' + s for s in home_batting_oldkeys]
    home_batting_vals = list(home_batting_dict.values())
    home_batting_dict = {k: v for k, v in zip(home_batting_newkeys, home_batting_vals)}


    dicts = {**gamedata_dict, **datetime_dict, **status_dict, **gameinfo_dict, **venue_dict_filtered, 
        **fieldinfo_dict, **weather_dict, **away_dict_filtered, **home_dict_filtered, **pitcher_dict, **away_pitching_dict, **home_pitching_dict,
        **away_batting_dict, **home_batting_dict}

    game_df = pd.DataFrame(dicts,index=[0])

    print(game_df[['pitcher_winner_playerid', 'pitcher_winner_playername']])

    game_df_list.append(game_df)


# RUN FOR LOOP TO GET REMAINING GAMES, THEN JOIN WITH PARQUET FILE BELOW TO GET ALL DATA IN ONE FILE

game_df_complete = pd.concat(game_df_list)
game_df_table = pa.Table.from_pandas(game_df_complete, preserve_index=False)
pq.write_table(game_df_table, 'projects/mlb-fantasy/mlb-dk-get-data/data/raw/season_n_playoff_game_data_2001_to_2012_a.parquet')



# get player stats for each gamepk ------------------------------------------------------------------

## need to break into 2 periods since computer shuts down with more than that 
batter_stats_list = []
pitcher_stats_list = []
gameboxinfo_list = []
missing_gamebox_list = []
pk_w_missing_player_list = []


for pk in rel_game_pks:
    print(pk) 

    try:
        statsapi.boxscore_data(pk, timecode=None)
    except KeyError:
        print('Key error for: '+pk)
        pk_w_missing_player_list.append(pk)
        continue

    boxscore = statsapi.boxscore_data(pk, timecode=None)

    # player info -----------------------------------
    if len(boxscore['playerInfo']) == 0:
        missing_gamebox_list.append(pk)
        print(pk, "no player info")
        continue
    
    player_info_list = []
    player_info_dict = boxscore['playerInfo']
    
    for i in player_info_dict:
        single_game_player_dict = {}
        single_game_player_dict['person_id'] = str(player_info_dict[i]['id'])
        single_game_player_dict['boxscore_player_name'] = player_info_dict[i]['boxscoreName']
        player_info_list.append(single_game_player_dict)

    player_info_df = pd.DataFrame(player_info_list)
    player_info_df['gamepk'] = pk

    # batter stats ----------------------------------
    away_batter_stats = pd.DataFrame(boxscore['awayBatters'])
    away_batter_stats['teamname'] = boxscore['awayBatters'][0]['namefield']
    home_batter_stats = pd.DataFrame(boxscore['homeBatters'])
    home_batter_stats['teamname'] = boxscore['homeBatters'][0]['namefield']

    batter_stats_complete = pd.concat([away_batter_stats, home_batter_stats], axis=0)
    batter_stats_complete_wpersonid = batter_stats_complete.merge(player_info_df, how='left', left_on='name', right_on='boxscore_player_name')


    # pitching stats -------------------------

    away_pitcher_stats = pd.DataFrame(boxscore['awayPitchers'])
    away_pitcher_stats['teamname'] = boxscore['awayPitchers'][0]['namefield']
    home_pitcher_stats = pd.DataFrame(boxscore['homePitchers'])
    home_pitcher_stats['teamname'] = boxscore['homePitchers'][0]['namefield']
    
    pitcher_stats_complete = pd.concat([away_pitcher_stats, home_pitcher_stats], axis=0)
    pitcher_stats_complete['gamepk'] = pk

    gameboxinfo_df = pd.DataFrame(boxscore['gameBoxInfo'])
    gameboxinfo_df['gamepk'] = pk

    # just need to create a dataframe with two columns: player name + value, then join to pitcher stats
    # also need to think whether you can get who will be pitching that day or who will be a part of the bullpen
    
    ## rel_gameboxinfo_values = ['Pitches-strikes','Groundouts-flyouts', 'Batters faced']
    ## pitches_strikes_str = gameboxinfo_df[gameboxinfo_df['label'] == 'Pitches-strikes']['value']

    batter_stats_list.append(batter_stats_complete_wpersonid)
    pitcher_stats_list.append(pitcher_stats_complete)
    gameboxinfo_list.append(gameboxinfo_df)



batter_stats_df = pd.concat(batter_stats_list)
batter_stats_table = pa.Table.from_pandas(batter_stats_df, preserve_index=False)
#pq.write_table(batter_stats_table, 'projects/mlb-fantasy/data/raw/batter_boxscore_stats_2013_to_2022.parquet')

pitcher_stats_df = pd.concat(pitcher_stats_list)
pitcher_stats_table = pa.Table.from_pandas(pitcher_stats_df, preserve_index=False)
#pq.write_table(pitcher_stats_table, 'projects/mlb-fantasy/data/raw/pitcher_boxscore_stats_2013_to_2022.parquet')

gameboxinfo_df = pd.concat(gameboxinfo_list)
gameboxinfo_table = pa.Table.from_pandas(gameboxinfo_df, preserve_index=False)
#pq.write_table(batter_stats_table, 'projects/mlb-fantasy/data/raw/gamebox_boxscore_stats_2013_to_2022.parquet')

missing_gamebox_df = pd.DataFrame(missing_gamebox_list)
missing_gamebox_table = pa.Table.from_pandas(missing_gamebox_df, preserve_index=False)
#pq.write_table(missing_gamebox_table, 'projects/mlb-fantasy/data/raw/missing_gamebox_2013_to_2022.parquet')


# so we have 4 tables: game data, batter stats, pitcher stats, and gamebox info
# need to remove ' Batters' and ' Pitchers' from team name

# write data to s3 ----------------------------------------------------------------
import boto3

boxscore_data_2001_to_2012 = pq.read_table('projects/mlb-fantasy/data/raw/boxscore_stats_2001_to_2012.parquet').to_pandas()

wr.s3.to_parquet(
    df=boxscore_data_2001_to_2012,
    path="s3://mlbdk-model/season_playoff_game_details/historical/boxscore_stats_2001_to_2012.parquet"
)

boxscore_data_2013_to_2022 = pq.read_table('projects/mlb-fantasy/data/raw/boxscore_stats_2013_to_2022.parquet').to_pandas()

wr.s3.to_parquet(
    df=boxscore_data_2013_to_2022,
    path="s3://mlbdk-model/season_playoff_game_details/historical/boxscore_stats_2013_to_2022.parquet"
)

# APPENDIX -------------------------------------------------------------------------------------

# code to get games before, doesn't work because some games don't have scores so it key error's out when looping


# get schedule, score, and gameids ----------------------------------------------
sched_complete = []

for season, start_date, end_date in zip(seasons, start_dates, end_dates):
    print(season, start_date)
    sched = statsapi.get(start_date=start_date, end_date=end_date)
    sched_complete.append(sched)
    print(season)

## we're able to grab some schedules but some erorr out with a KeyError 'score': 2000, 2002, 2003, 2004, 2006, 2011
sched = statsapi.schedule(start_date='01/01/{}'.format(2022) ,end_date='01/01/{}'.format(2023))

game_ids = [d['game_id'] for d in sched]
game_datetimes = [d['game_datetime'] for d in sched]
game_types = [d['game_type'] for d in sched]
game_status = [d['status'] for d in sched]
away_ids = [d['away_id'] for d in sched]
home_ids = [d['home_id'] for d in sched]
doubleheaders = [d['doubleheader'] for d in sched]
game_num = [d['game_num'] for d in sched]
venue_ids = [d['venue_id'] for d in sched]
away_scores = [d['away_score'] for d in sched]
home_scores = [d['home_score'] for d in sched]

schedule_game_dict = {'game_id': game_ids, 'game_datetime': game_datetimes, 'game_type': game_types, 'game_status': game_status,
    'away_id': away_ids, 'home_id': home_ids, 'doubleheader': doubleheaders, 'game_num': game_num,
    'venue_id': venue_ids, 'away_score': away_scores, 'home_score': home_scores}

schedule_game_df = pd.DataFrame(schedule_game_dict)

