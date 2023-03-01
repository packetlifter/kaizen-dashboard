import gspread
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import NotFoundError
import datetime
import yaml
import sys

def main(config_file):

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    es = Elasticsearch(config['es_host'],basic_auth=(config['es_username'],config['es_password'],),verify_certs=False)    
    try:
        es.indices.delete(index=config['es_index_name'])
    except NotFoundError:
        pass

    squammies_dict = {}

    sa = gspread.service_account(filename=config['gssa_file'])
    for spreadsheet in sa.list_spreadsheet_files():
        squammies_dict[spreadsheet['name']] = {}

        kaizen_sheet = sa.open(spreadsheet['name'])

        for week in kaizen_sheet:
            
            if not week.title.startswith('W'):
                continue
            
            row_values = week.get_all_values()
            
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
                        spreadsheet_data_clean[key] = [ item*2.2 if item < 100 else item for item in actual_data]
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
                
                dict_item['week'] = week.title.replace('W','Week ')
                dict_item['member_name'] = shortened_name(spreadsheet['name'],config['name_mappings'])
                
                if week.title != 'W1':
                
                    weight_change = row_values[1][3]
                    percent_change = weight_change.split()[1].replace('(','').replace(')','').replace('%','')
                    dict_item['weight_change'] = float(percent_change)
                
                week_score = row_values[70][6]
                percent_score = week_score.replace('%','')
                dict_item['week_score'] = float(percent_score)
                
                kaizen_data.append(dict_item)
        
            actions = [
            {
                "_index": config['es_index_name'],
                "_source": item,
                
            }
            for item in kaizen_data
            ]
        
    
            bulk(es, actions)
        time.sleep(10)    
        
def shortened_name(name,mappings):
    

    short_name = name.replace("[Kaizen S3] ", "")  # removes "[Kaizen S3] "
    short_name = short_name.replace(" (v4.1)", "")  # removes " (v4.1)"
    for k,v in mappings.items():
        if k in short_name:

            short_name = v

    return short_name

if __name__ == '__main__':
    main(sys.argv[1])
