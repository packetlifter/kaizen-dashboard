import gspread
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import NotFoundError
import datetime
import yaml
import sys
import logging
import urllib3
urllib3.disable_warnings()

logging.basicConfig(filename='dashboard.log', filemode='w',format='%(asctime)s - %(message)s',level=logging.INFO)

def main(config_file, init):
    

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    es = Elasticsearch(config['es_host'],basic_auth=(config['es_username'],config['es_password'],),verify_certs=False)    


    squammies_dict = {}

    sa = gspread.service_account(filename=config['gssa_file'])

    ss_weeks = get_kaizen_weeks(init)

    if len(ss_weeks) < 2:
                
        index_name = config['es_index_name'] + "-current_week"
    else:
        index_name = config['es_index_name'] + "-init"
 
    try:
        es.indices.delete(index=index_name)
    except NotFoundError:
        pass

    for spreadsheet in sa.list_spreadsheet_files():
        squammies_dict[spreadsheet['name']] = {}

        kaizen_sheet = sa.open(spreadsheet['name'])

        for sheet in kaizen_sheet:

            if sheet.title == 'Setup':
                
                weight_setup = sheet.acell('E21').value
            
            if not sheet.title in ss_weeks:
                continue
            
            row_values = sheet.get_all_values()
            
            spreadsheet_data = {
                "days": row_values[11],
                "dates": row_values[12],
                "sleep": row_values[13],
                "weigh_ins": row_values[15],
                "calories": row_values[25],
                "protein": row_values[27],
                "steps": row_values[29],
                "stress": row_values[38],
                "fatigue": row_values[40],
                "hunger": row_values[42]
            }
            
            spreadsheet_data_clean = {}
            floats_data = ["sleep","weigh_ins","stress","fatigue","hunger" ]
            ints_data = ["calories", "protein", "steps"]

            for key, data in spreadsheet_data.items():
                actual_data = data[2:9]
                if key == 'days':
                    spreadsheet_data_clean['days'] = [ item.strip() for item in actual_data]
                    continue
                if key == 'dates':
                    spreadsheet_data_clean['dates'] = []
                    for date in actual_data:
                        
                        date_obj = datetime.datetime.strptime(date, "%m/%d/%Y")
                        date_str = date_obj.strftime("%Y-%m-%d")
                        spreadsheet_data_clean['dates'].append(date_str)
                    continue

                actual_data = [ -1 if item == '' else item for item in actual_data] #remove blank data

                if key in floats_data:
                    actual_data = list(map(float,actual_data)) # convert to float
                    
                    if key == 'weigh_ins':             
                        spreadsheet_data_clean[key] = [ item*2.2 if weight_setup  == 'kg' else item for item in actual_data]
                    else:
                        spreadsheet_data_clean[key] = actual_data

                elif key in ints_data:
                    if key == 'calories':
                        spreadsheet_data_clean[key] = [int(item.replace(' cals', '').replace(',','').strip()) if item != -1 else item for item in actual_data]
                    elif key == 'protein':
                        spreadsheet_data_clean[key] = [int(item.replace(' g', '').strip()) if item != -1 else item for item in actual_data]    
                    elif key == 'steps':
                        spreadsheet_data_clean[key] = [int(item.replace(" steps", '').replace(',','').strip()) if item != -1 else item for item in actual_data]
                    
            tracker_values =  [ item for item in spreadsheet_data_clean.values()]
            tracker_keys = [ item for item in spreadsheet_data_clean.keys()]
            kaizen_data = []
            

            for item in zip(spreadsheet_data_clean['days'],
                            spreadsheet_data_clean['dates'],
                            spreadsheet_data_clean['sleep'],
                            spreadsheet_data_clean['weigh_ins'],
                            spreadsheet_data_clean['calories'],
                            spreadsheet_data_clean['protein'],
                            spreadsheet_data_clean['steps'],
                            spreadsheet_data_clean['stress'],
                            spreadsheet_data_clean['fatigue'],
                            spreadsheet_data_clean['hunger']):
                
                dict_item = dict(zip(tracker_keys, item))
                
                dict_item['week'] = sheet.title.replace('W','Week ')
                dict_item['member_name'] = shortened_name(spreadsheet['name'],config['name_mappings'])
                #if dict_item['member_name'] == 'Marla':
                #    import ipdb; ipdb.set_trace()
                if sheet.title != 'W1':
                
                    weight_change = row_values[1][3]
                    percent_change = weight_change.split()[1].replace('(','').replace(')','').replace('%','')
                    dict_item['weight_change'] = float(percent_change)
                
                week_score = row_values[70][6]
                percent_score = week_score.replace('%','')
                dict_item['week_score'] = float(percent_score)
                
                kaizen_data.append(dict_item)

            
                
        
            actions = [
            {
                "_index": index_name,
                "_source": item,
                
            }
            for item in kaizen_data
            ]
        
    
            bulk(es, actions)

    logging.info(f"Pushed data for Team {config['es_index_name']}") 
        
def shortened_name(name,mappings):
    

    short_name = name.replace("[Kaizen S3] ", "")  # removes "[Kaizen S3] "
    short_name = short_name.replace(" (v4.1)", "")  # removes " (v4.1)"
    for k,v in mappings.items():
        if k in short_name:

            short_name = v

    return short_name

def get_kaizen_weeks(init):

    weeks_map = {
        "04": "W1",
        "05": "W2",
        "06": "W3",
        "07": "W4",
        "08": "W5",
        "09": "W6",
        "10": "W7",
        "11": "W8",
        "12": "W9",
        "13": "W10",
        "14": "W11",
        "15": "W12",
        "16": "W13",
        "17": "W14",
        "18": "W15",
    }

    # Define the start and end dates for the range of weeks you want to create
    start_date = datetime.date(2023, 1, 23)
    end_date = datetime.date(2023, 5, 7)
    
    # Create an empty dictionary to store the weeks and their corresponding dates
    weeks_dict = {}
    
    # Loop through each week in the range of dates and add it to the dictionary
    while start_date <= end_date:
        week_start = start_date - datetime.timedelta(days=start_date.weekday())  # Get the Monday of this week
        week_num = week_start.strftime("%U")  # Get the week number as a string
        week_num = weeks_map[week_num]
        if week_num not in weeks_dict:
            weeks_dict[week_num] = []  # Create an empty list for this week
        weeks_dict[week_num].append(start_date.strftime("%m/%d/%Y"))  # Add the current date to the list
        start_date += datetime.timedelta(days=1)  # Move to the next day
    
    current_date = datetime.date.today().strftime("%m/%d/%Y")

    kaizen_weeks_list = []

    for week, days in weeks_dict.items():
        kaizen_weeks_list.append(week)
        if current_date in days:
            current_week = week

    if init:
        end_index = kaizen_weeks_list.index(current_week)
        weeks_list = kaizen_weeks_list[:end_index]
        return weeks_list 
    else:
    
        week_now = []
        week_now.append(current_week)
        return week_now
    

    
if __name__ == '__main__':

    if len(sys.argv) == 3:
        main(sys.argv[1],sys.argv[2])
    else:
        main(sys.argv[1],None)
    
