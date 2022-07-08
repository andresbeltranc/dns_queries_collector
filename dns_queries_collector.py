import requests
import pandas as pd
import json
import sys
from dateutil.parser import parse

URL = "https://api.lumu.io"
LUMU_CLIENT_KEY = "d39a0f19-7278-4a64-a255-b7646d1ace80"
COLLECTOR_ID = "5ab55d08-ae72-4017-a41c-d9d735360288"
import time

# make a request to the API Collector, it has the queries variable as a parameter, which is a list of dictionaries.
# return true if the request was successful otherwise returns false.
def query_request(queries):
    try:
        url = URL+"/collectors/"+COLLECTOR_ID+"/dns/queries"
        headers = {'Content-Type': 'application/json'}
        params = {'key':LUMU_CLIENT_KEY}
        response = requests.request("POST",url, headers=headers,params=params, data=json.dumps(queries))
        if response.status_code == 200:
            return True
        else:
            return False
    except Exception as e: 
        print(e)

# receives groups queries that are going to be process by query_request
def send_requests(chunked_queries):
    success = 0
    error = 0
    try:
        for queries in chunked_queries:
            reponse = query_request(queries)
            if reponse == False:
                error = error + 1
                print("Error Request")
            success = success + 1
        return success, error
    except Exception as e:
        print(e)

# create groups of 500 queries each,the number of groups depends on how much data is entered.
def chunk_queries(list_queries):
    try:
        return [list_queries[k:k+500] for k in range(0, len(list_queries), 500)]
    except Exception as e:
        print(e)


# converts the date format identified by pandas and converts it to iso format and UTC      
def iso_format_date(date_time_str):
    try:
        date = parse(date_time_str)
        return date.isoformat()+"Z"
    except Exception as e:
        print(e)

# receive a pandas dataframe which will be parsed with the correct format
# to be sent to the Lumu Collector Api, it also calculates the statistics 
# requested by Lumu Technologies from Ip and host.
def parse_queries(queries_df):
    listParsedQueries = []
    statistic_ip = dict()
    statistic_host = dict()
    #queries_df = queries_df.drop(queries_df.columns[[2,3,4,7,8,10,12,13]],axis = 1)
    try:
        for query in queries_df:
            client_ip = str(query[6]).split("#")[0]
            client_name =str(query[5]).replace("@","")
            date = iso_format_date(query[0]+" "+query[1])
            if client_ip in statistic_ip:
                count = statistic_ip[client_ip][0] + 1 
                percentage_key = "{:.2f}".format(round(count / len(queries_df)*100, 2))+"%"
                statistic_ip[client_ip] = [count,percentage_key]
            else:
                statistic_ip[client_ip] = [1, "{:.2f}".format(round((1/len(queries_df)*100), 2))]

            if query[9] in statistic_host:
                count = statistic_host[query[9]][0] + 1 
                percentage_key = "{:.2f}".format(round(count / len(queries_df)*100, 2))+"%"
                statistic_host[query[9]] = [count,percentage_key]
            else:
                statistic_host[query[9]] = [1, "{:.2f}".format(round((1/len(queries_df)*100), 2))]

            dict_query = {"timestamp":date,"name":str(query[9]),"client_ip":str(client_ip),"client_name":str(client_name),"type":str(query[11])}
            listParsedQueries.append(dict_query)
        return listParsedQueries, statistic_ip, statistic_host
    except Exception as e:
        print(e)


# receive the path of the file which will be open and transform in a pandas dataframe using the " " as delimiter.
def read_file(path):
    try:
        queries_df = pd.read_csv(path, delim_whitespace=True, header=None)
        return queries_df.values.tolist()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == '--pathfile':
        print("Starting...")
        queries_df = read_file(args[1])
        start_time = time.time()
        parsed_queries, statistic_ip, statistic_host = parse_queries(queries_df)
        statistic_sorted_ip = sorted(statistic_ip, key = statistic_ip.get, reverse=True)[:5]
        statistic_sorted_host = sorted(statistic_host, key = statistic_host.get, reverse=True)[:5]
        chunked_queries = chunk_queries(parsed_queries)
        print("------------------------------------")
        print("Total Records ",len(parsed_queries))
        print(" ")
        print("Client IPs Rank")
        print("------------------------------------")
        ips = []
        count = []
        percentage = []
        for s_ip in statistic_sorted_ip:
            ips.append(s_ip)
            count.append(statistic_ip[s_ip][0])
            percentage.append(statistic_ip[s_ip][1])
        dict_ip = {
            "IP":ips,
            "count":count,
            "percentage":percentage
        }
        df_ip = pd.DataFrame(dict_ip)
        print(df_ip)
        print("------------------------------------")
        print(" ")

        print("Host Rank")
        print("------------------------------------")
        hosts = []
        count_h = []
        percentage_h = []
        for s_host in statistic_sorted_host:
            hosts.append(s_host)
            count_h.append(statistic_host[s_host][0])
            percentage_h.append(statistic_host[s_host][1])
        
        dict_host = {
            "Host":hosts,
            "count":count_h,
            "percentage":percentage_h
        }
        df_host = pd.DataFrame(dict_host)
        print(df_host)
        queries_time = time.time() - start_time
        print("time it takes to process queries :","--- %s seconds ---" % (queries_time))
        print("------------------------------------")
        print("Queries send to Custom Collector API")
        print(" ")
        success, error = send_requests(chunked_queries)
        requests_time = time.time() - start_time - queries_time
        print("time it takes to send request :","--- %s seconds ---" % (requests_time))
        print("requests sent successfully :",success)
        print("requests sent unsuccessfully :",error)
        print("------------------------------------")
        print("total time :",queries_time+requests_time)
    else:
        print("incorrect arguments, see the README file")
