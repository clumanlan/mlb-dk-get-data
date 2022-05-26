import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import statsapi
from datetime import datetime, timedelta
import awswrangler as wr
import boto3 
import json
import time
import os

def get_secret():

    secret_name = "dkuser_aws_keys"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    secret_response = get_secret_value_response['SecretString']

    return json.loads(secret_response)
    


def get_most_recent_date():

    secret_dict = get_secret()
    aws_key_id = secret_dict['aws_access_key_id']
    aws_secret = secret_dict['aws_secret_access_key']

    s3_resource = boto3.resource('s3',
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret)

    bucket_name = 'mlbdk-model'
    bucket = s3_resource.Bucket(bucket_name)
    key = 'season_playoff_game_details/'
    objs = bucket.objects.filter(Prefix=key)

    season_files = {}

    for file in objs:
        key = file.key
        last_modified = file.last_modified
        season_files[key] = last_modified

    season_file_df = pd.DataFrame(season_files.items(), columns=['key', 'last_modified_date'])
    
    file_date = season_file_df['last_modified_date'].max().date().strftime('%Y-%m-%d')
    
    return file_date



# get most recent game pks----------------------

def get_season_n_playoff_gamepks(start_date):

    game_pk_list = []
    game_type_list = []
    season_list = []

    end_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d') 

    sched = statsapi.get("schedule", {"sportId": 1, "startDate": start_date, "endDate": end_date, "fields": "dates,date,games,gamePk, gameType, season"}) #gametype parameter doesn't work

    game_list = sched['dates']

    for date_list in game_list:
        game_dicts = date_list['games'] # returns a list of game dicts binned by date
        
        for game in game_dicts:
            game_pk_list.append(game['gamePk'])
            game_type_list.append(game['gameType'])
            season_list.append(game['season'])

    game_pk_df = pd.DataFrame({'game_pk':game_pk_list, 'game_type':game_type_list, 'season':season_list})
    rel_game_pks = game_pk_df[game_pk_df['game_type'].isin(['R', 'F', 'D', 'L', 'W'])]['game_pk'] # filter for season/playoff games

    return rel_game_pks


# get basic game info: time, place, duration  --------------------------------------------------------------------
def get_game_info(rel_game_pks):
    game_df_list = []

    for pk in rel_game_pks:
        print(pk)
    
        game = statsapi.get("game", {"gamePk":pk})
    
        end_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d') 

        if game['gameData']['datetime']['officialDate'] > end_date: # some future rescheduled games get snuck in to rel_game_pks
            print('Future Game Skipped: ')
            print(pk)
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

        game_df_list.append(game_df)

    game_df_complete = pd.concat(game_df_list)

    return game_df_complete

# get player boxscore stats for each gamepk ------------------------------------------------------------------

def get_player_boxscore_stats(rel_game_pks):

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

        gameboxsummary_df = pd.DataFrame(boxscore['gameBoxInfo'])
        gameboxsummary_df['gamepk'] = pk

        print('batter boxscore rows: ' + str(batter_stats_complete_wpersonid.shape[0]))
        print('pitcher boxscore rows: ' + str(pitcher_stats_complete.shape[0]))

        batter_stats_list.append(batter_stats_complete_wpersonid)
        pitcher_stats_list.append(pitcher_stats_complete)
        gameboxinfo_list.append(gameboxsummary_df)

        time.sleep(1.3)
    
    batter_stats_df = pd.concat(batter_stats_list)
    pitcher_stats_df = pd.concat(pitcher_stats_list)
    gameboxsummary_df = pd.concat(gameboxinfo_list)
    missing_gamebox_df = pd.DataFrame(missing_gamebox_list)

    return batter_stats_df, pitcher_stats_df, gameboxsummary_df, missing_gamebox_df



def write_data_to_s3(game_df_complete, batter_stats_df, pitcher_stats_df, gameboxsummary_df, missing_gamebox_df):

    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    secret_dict = get_secret()
    aws_key_id = secret_dict['aws_access_key_id']
    aws_secret = secret_dict['aws_secret_access_key']

    session = boto3.Session(
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret)
    
    wr.s3.to_parquet(
            df=game_df_complete,
            path="s3://mlbdk-model/season_playoff_game_details/season_playoff_game_data_{}.parquet".format(current_date),
            boto3_session=session
        )


    wr.s3.to_parquet(
            df=batter_stats_df,
            path="s3://mlbdk-model/batter_boxscore_stats/batter_boxscore_stats_{}.parquet".format(current_date),
            boto3_session=session
        )

    wr.s3.to_parquet(
            df=pitcher_stats_df,
            path="s3://mlbdk-model/pitcher_boxscore_stats/pitcher_boxscore_stats_{}.parquet".format(current_date),
            boto3_session=session
        )

    wr.s3.to_parquet(
            df=gameboxsummary_df,
            path="s3://mlbdk-model/gamebox_summary/gamebox_summary_{}.parquet".format(current_date),
            boto3_session=session
        )

    if not missing_gamebox_df.empty:
        wr.s3.to_parquet(
            df=missing_gamebox_df,
            path="s3://mlbdk-model/missing_gamebox/missing_gamebox_{}.parquet".format(current_date),
            boto3_session=session
        )


def handler(event, context):
    
    yesterday = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d') 
    last_date_pulled = get_most_recent_date()

    if last_date_pulled >= yesterday:
        return { 
        'message' : 'Game data up to date.'
    }

    rel_game_pks = get_season_n_playoff_gamepks(last_date_pulled)
    game_df_complete = get_game_info(rel_game_pks)
    batter_stats_df, pitcher_stats_df, gameboxsummary_df, missing_gamebox_df = get_player_boxscore_stats(rel_game_pks)

    time.sleep(45) # sleep so previous function has enough time to write to disk

    write_data_to_s3(game_df_complete, batter_stats_df, pitcher_stats_df, gameboxsummary_df, missing_gamebox_df)

handler(None, None)

