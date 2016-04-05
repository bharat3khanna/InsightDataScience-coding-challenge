import json
import datetime

# Function that creates a python dictionary from a list
# For example : ['Apache,'Hadoop'] gets converted to {'Apache':'Hadoop', 'Hadoop':'Apache'}
def create_hashmap(tags):
    hashmap = {}
    key = ''
    for i in range(len(tags)):
        key = tags[i]
        values = []
        for value in tags:
            if key != value:
                values.append(value)
        hashmap[key] = values
    return hashmap

# Function to build/update  big hash map that is a dictionary to store all hash tags
# Input  :  dictionary that has been created using create_hashmap function
# Output :  Big hash map is updated if keys are already present else keys and their values are inserted in big hash map
def build_hashmap(hashmap):

    for key, values in hashmap.items():
        # values = []
        if key in big_hash_map:
            if len(hashmap[key]) != 0:
                for val in values:
                    if val not in big_hash_map[key]:
                        big_hash_map[key].append(val)

        else:
             big_hash_map[key] = hashmap[key]
    # return big_hash_map

# Function to calculate degree of  big hash map
# Input : None
# Output : Float value (upto 2 decimal place) that tells you degree of big hash map
def get_degree():
    data = big_hash_map
    sum = 0
    degree = 0
    for key in data:
        count = 0
        # count = sum([1 for x in data[key]])
        count = len(data[key])
        if count > 0:
            sum += count
    # print sum
    if key in data and data[key]:
        if len(data) != 0:
            if sum % len(data):
                degree = sum / float(len(data))
            else:
                degree = sum / len(data)
        else:
            return 0

    return "{0:.2f}".format(degree)


# Function to calculate time difference between an incoming tweet and minimum/maximum time value of already inserted tweets using timelist dictionary
# Input     : Data type : Date, Value : Date of incoming's tweet's created at field
# Output    : Data type : String, Value : In Values ("update", "insert", "discard")
# "update"  : The function sets this flag when difference between incoming's tweet's date and minimum time stamp stored in time list dictionary
#             is more than 60 seconds and its difference with maximum time minimum time stamp stored in time list dictionary is less than 60 seconds
# "insert"  : The function sets this flag when both the differences between incoming's tweet's date and minimum/maximum time stamp stored in time list
#             dictionary is less than 60 seconds
# "discard" : The function sets this flag when incoming tweets' date is more than 60 seconds of maximum time stamp stored in time list dictionary

def getTimeDiff(date):
        min_value = min(time_list.itervalues()) # get the min value in time dictionary
        max_value = max(time_list.itervalues()) # get the max value in time dictionary
        delta_max = (max_value - date).seconds
        delta_min = (date - min_value).seconds
        # print "Delta min:", delta_min
        # print "Delta max:", delta_max
        if delta_max < 60:
            if delta_min > 60:
                return "update"
            # remove values in hashmap based on keys
            # return False
            else:
                return "insert"
        else:
            return "discard"

# Function to update big hash map i.e. it removes values ( list of hash tags) from keys (hash tag) when flag returned by getTimeDiff function is "update"
# Input : Data type : Dictionary, Value : hashmap made by function create_hashmap based on hash tags to be removed
# Output : None, Big hash map is updated by removing the key's values
def update_hashmap_elements(hashmap):
    # print "big hash map before merging :", big_hashmap
    # print "hashmap to be merged :", hashmap
    for key, values in hashmap.items():
        # values = []
        if key in big_hash_map:
            if len(hashmap[key]) != 0:
                for val in values:
                    if val in big_hash_map[key]:
                        big_hash_map[key].remove(val)


    # return big_hash_map
    # print "big hash map after merging :", big_hashmap

# Function used to update big hash map dictionary by calling update hashmap_elements and also to update timelist dictionary by removing the keys
# based on flag returned by getTimeDiff
# Input : Data type : list , Values : keys got from time list dictionary that needs to be removed from time list dictionary
# Output : None, Big hash map as well time list dictionary is updated with appropriate keys and values
def update_hash_map(ids):
    hash_tags = []
    for id in ids:
        if id in hash_tags_list:
            try:
                hash_tags = hash_tags_list[id]
                hashmap = create_hashmap(hash_tags)
                # print "hashmap :", hashmap
                update_hashmap_elements(hashmap)
                try:
                    del hash_tags_list[id]
                except KeyError:
                    print "Key not found"
            except Exception as e:
                print "Error while updating hash tags list :", e.message


# Main program starts from here

if __name__ == '__main__':
    # start = datetime.datetime.now()
    tweets_file_path = "./tweet_input/tweets.txt"# path of the file where tweets have been loaded
    tweets_write_path = "./tweet_output/output.txt"# path of the file where degree after each tweet is processed will be written
    big_hash_map = {}  # dictionary to store all hash tags, here hash tags will be stored in form of key value pairs for e.g. {'Apache':['Hadoop','Storm]}
    time_list = {}    # dictionary to store tweet id's as keys and tweet's created date as value for e.g. {714593712307838977: datetime.datetime(2016, 3, 28, 23, 23, 42)}
    hash_tags_list = {} # dictionary to store tweet id's as keys and hash tags as values for e.g. {714593712307838977: [u'Flink', u'Spark']}
    degree_out = open(tweets_write_path, "w") # file object to write degree to tweet output file
    

    with open(tweets_file_path, 'r') as tweets_file: #file object to read tweets from tweets input file

        for line in tweets_file: # loop to process each tweet line by line
            # print line
            try:
                tweet = json.loads(line) #json module  is used to parse tweets
                if tweet.get('limit'): # if limit message is found, ignore it
                    continue
                if 'text' in tweet:
                    id = tweet['id'] # gets the tweet id
                    created_at = tweet['created_at'] # gets the tweet date

                    created_date = datetime.datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y') #convert the tweet date from string to date data type
                    time_list[id] = created_date # store the id and date as key value pair in time_list dictionary
                    # get the flag value based on difference newly fetched tweet and existing tweets in time_list dictionary
                    flag = getTimeDiff(created_date)

                    while flag == "update":# keep on removing minimum values as long as time difference between max process tweet is greater than 60
                        min_value = min(time_list.itervalues()) # get the minimum datetime stored in timelist dictionary
                        min_keys = [k for k in time_list if time_list[k] == min_value] #get the minimum keys based on min values stored in timelist dictionary
                        update_hash_map(min_keys) # update big hash map by removing these keys
                        try:
                            map(time_list.pop, min_keys) # update time list by removing these keys
                        except KeyError:
                            print "Key not found"
                        flag = getTimeDiff(created_date)

                    if flag == "discard": # if out of order tweet arrives and its time difference with maximum time processed is more than 60 seconds
                        degree = get_degree() # calculate the degree
                        degree_out.write(degree) # write the degree to output file
                        degree_out.write("\n")

                    elif flag == "insert": # if time difference is less than 60 seconds insert it
                        time_list[id] = created_date
                        hash_tags = []
                        for hashtag in tweet['entities']['hashtags']: # extract hash tags from tweet
                            hash_tags.append(hashtag['text'])
                            hash_tags = list(set(hash_tags))# remove self loops / duplicate values if any
                        if len(hash_tags) > 0:
                            hash_tags_list[id] = hash_tags
                            hash_map = create_hashmap(hash_tags) # create a dictionary based on hash tags (list)
                            build_hashmap(hash_map) # insert this newly created hash map in big hash map
                        degree = get_degree() # calculate the degree
                        degree_out.write(degree) # write the degree to output file
                        degree_out.write("\n")


            except Exception as e:
                   print "Error while paring tweet, Error message :", e.message, "in tweet :\n", tweet
                   

        # print "Final hash tags list :", hash_tags_list
        # print "big hash map :", big_hash_map
        # print get_degree()

        # end = datetime.datetime.now()
        # diff = (end - start).seconds
        # print "Time taken to process is seconds :", diff
