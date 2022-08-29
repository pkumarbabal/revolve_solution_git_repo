import argparse
import pandas as pd
import os
import json


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="../../input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="../../input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="../../input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="../../output_data/outputs/")
    return vars(parser.parse_args())

def read_csv(file_path):
    return pd.read_csv(file_path)

def get_directory_sructure(file_path):
    return os.listdir(file_path)

def process_week_data(dir_structure, product_data_dict, customer_data_dict,path):
    week=1
    last_week_day=week
    last_day=week
    temp_dict_json={}
    dict_json_list=[]
    dates_list_week=[]

    for day in dir_structure:
        raw_read_json=pd.read_json(path+day+'/'+'transactions.json',lines=True).to_dict()
        process_json_into_dict(raw_read_json,product_data_dict,customer_data_dict,temp_dict_json)
        last_day=week
        if week%7==0:
            dates_list_week.append(dir_structure[week-7][2:]+'_to_'+dir_structure[week-1][2:])
            dict_json_list.append(temp_dict_json)
            temp_dict_json={}
            last_week_day=week-1
        week+=1

    if (week-1)%7!=0:
        dict_json_list.append(temp_dict_json)
        dates_list_week.append(dir_structure[last_week_day][2:]+'_to_'+dir_structure[last_day-1][2:])

    return dict_json_list,dates_list_week

def process_json_into_dict(raw_read_json,product_data_dict,customer_data_dict,dict_json):
    product_category_list=list(product_data_dict['product_category'].values())
    customer_loyalty_list=list(customer_data_dict['loyalty_score'].values())
    for row_num in raw_read_json['customer_id'].keys():
        current_customer = raw_read_json['customer_id'][row_num]
        if dict_json.get(current_customer) == None:
            dict_json[current_customer]={}
            dict_json[current_customer]['loyalty_score']=customer_loyalty_list[row_num]
        for purchases_cc in raw_read_json['basket'][row_num]:
            current_product_id=purchases_cc['product_id']
            if dict_json[current_customer].get(current_product_id)==None:
                dict_json[current_customer][current_product_id]={}
                dict_json[current_customer][current_product_id]['product_id']=current_product_id
                dict_json[current_customer][current_product_id]['purchase_count']=1
                dict_json[current_customer][current_product_id]['product_category']=product_category_list[int(current_product_id[1:])-1]
            else:
                dict_json[current_customer][current_product_id]['purchase_count']+=1

def process_week_data_output(list_week_raw_dict):
    list_week_days_output_dict=[]
    list_week_output_dict=[]
    for raw_week_dict in list_week_raw_dict:
        list_week_days_output_dict=[]
        for customers in raw_week_dict.keys():
            dict_week_otput_dict={}
            dict_week_otput_dict['customer_id']=customers
            dict_week_otput_dict['loyalty_score']=raw_week_dict[customers]['loyalty_score']
            dict_week_otput_dict['products']=[]
            for products in raw_week_dict[customers].keys():
                if products != 'loyalty_score':
                    dict_week_otput_dict['products'].append(raw_week_dict[customers][products])
            list_week_days_output_dict.append(dict_week_otput_dict)
        list_week_output_dict.append(list_week_days_output_dict)


    return list_week_output_dict

def convert_to_dict(dataframe):
    return dataframe.to_dict()

def create_dir_output(list_week_output_dict,path,dates_list_week):
    for week_dict_number in range(len(list_week_output_dict)):
        new_path_weekwise=path+dates_list_week[week_dict_number]
        try:
            os.mkdir(new_path_weekwise)
        except:
            print(f"file already exists, skipping this step for {dates_list_week[week_dict_number]}")
        with open(new_path_weekwise+'/'+'output.json', 'w') as fout:
             for day_dict_number in range(len(list_week_output_dict[week_dict_number])):
                dictionary=list_week_output_dict[week_dict_number][day_dict_number]
                json.dump(dictionary , fout)
                fout.write('\n')

def main():
    params = get_params()
    Customer_data_raw = read_csv(params['customers_location'])
    customer_data_dict = convert_to_dict(Customer_data_raw)
    Product_data_raw = read_csv(params['products_location'])
    product_data_dict = convert_to_dict(Product_data_raw)
    dir_structure = get_directory_sructure(params['transactions_location'])
    list_week_raw_dict,dates_list_week=process_week_data(dir_structure, product_data_dict, customer_data_dict,params['transactions_location'])
    list_week_output_dict=process_week_data_output(list_week_raw_dict)
    create_dir_output(list_week_output_dict,params['output_location'],dates_list_week)

if __name__ == "__main__":
    main()
