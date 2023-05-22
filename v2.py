
from pprint import pprint, pformat
import time

# Initializing the dictionary.
uotable = {}
futable = {}
database = []
iteration = 0

str_list = []
list_results = []

# read database from database.txt
def read_database():
    """
    It reads the database file and returns a list of dictionaries
    :return: A list of dictionaries.
    """
    with open('DIfferent PSU BANK Count/DB_NIFTY_PSU_12.txt', 'r') as f:
        database = f.read().split('\n')
    
    # create a list of lists
    database = [line.split(':') for line in database]

    # remove empty list
    database = [line for line in database if line != ['']]

    db = []
    for i in range(len(database)):
        data = {}
        data["itemSet"] = database[i][0].split(' ')
        data["sum"] = float(database[i][1])
        data["transactionalUtility"] = database[i][2].split(' ')
        if database[i][1] != '0':
            db.append(data)

    return db

def UtilityOccupancyTableGeneration (database, list_of_items):
    """
    The function takes in a database and a list of items, and returns a list of two dictionaries. The
    first dictionary maps each item to a list of lists, where each list contains the transaction ID, the
    utility of the item in the transaction, and the remaining utility of the transaction. The second
    dictionary maps each item to a list of two values, the first being the average utility of the item
    in the database, and the second being the average remaining utility of the transaction.
    
    :param database: the database of transactions
    :param list_of_items: list of items in the database
    :return: mapperuo:
            key: item
            value: list of list
                list of list:
                    [tid, uo, uc]
        mapperfu:
            key: item
            value: list of list
                list of list:
                    [fu, fc]
    """
    mapperuo = {}
    mapperfu = {}

    # populate mapper
    for i in list_of_items:
        mapperuo[i] = []
        mapperfu[i] = []

    for row in database:
        sum_total = 0
        for index, item in enumerate(row["itemSet"]):
            tid = database.index(row) + 1
            sum_total += float(row["transactionalUtility"][index])
            mapperuo[item].append([
                tid,
                float((row["transactionalUtility"][index]))/float(row["sum"]),
                # int((row["transactionalUtility"][index])), # zero division error    
                (row["sum"] - sum_total)
            ])
    
    for key in mapperuo:
        l = mapperuo[key]
        
        # sum column index 1, 2
        sum_column1 = 0
        sum_column2 = 0
        for i in range(len(l)):
            sum_column1 += l[i][1]
            sum_column2 += l[i][2]
        mapperfu[key].append((sum_column1)/len(database))
        mapperfu[key].append(sum_column2/len(database))
    
    return [mapperuo, mapperfu]

def UtilityOccupancyTableUpdater(prefix, xa, xb):
    # Updating the utility occupancy table and the utility frequency table.
    xauo = uotable[xa]
    xbuo = uotable[xb]
    xafu = futable[xa]
    xbfu = futable[xb]

    len_trans = len(xauo)

    uolist = []
    fulist = []

    for index in range(len_trans):
        xauo_ = xauo[index]
        xbuo_ = xbuo[index]

        if len(prefix) == 0:
            uolist.append([
                xauo_[0],
                xauo_[1] + xbuo_[1],
                xbuo_[2]
            ])
        else:
            preuo = uotable[prefix]
            preuo_ = preuo[index]
            uolist.append([
                xauo_[0],
                xauo_[1] + xbuo_[1] - 2*preuo_[1],
                xbuo_[2]
            ])
    
    # sum column index 1, 2
    sum_column1 = 0
    sum_column2 = 0
    for i in range(len(uolist)):
        sum_column1 += uolist[i][1]
        sum_column2 += uolist[i][2]
    fulist.append((sum_column1)/len(database))
    fulist.append(sum_column2/len(database))

    uotable[prefix+xa[-1]+xb[-1]] = uolist
    futable[prefix+xa[-1]+xb[-1]] = fulist
    
    return prefix+xa[-1]+xb[-1]

def potentialItemsetCalculator (previous_itemset, potential_itemset):
    """
    The function takes in two lists, the previous itemset and the potential itemset, and returns the
    potential itemset with the items that are not in the previous itemset removed.
    
    :param previous_itemset: the previous itemset
    :param potential_itemset: the potential itemset
    :return: the potential itemset with the items that are not in the previous itemset removed
    """
    list_ = []
    for item in potential_itemset:
        if item not in previous_itemset:
            list_.append(previous_itemset+item)
    return list_

def upperBoundCalculator (itemset):
    sum_uo_ro = 0

    # join list of items
    str_itemset = itemset
    itemset_utility_list = uotable[str_itemset]

    # sum index 1 and 2
    for i in range(len(itemset_utility_list)):
        sum_uo_ro += itemset_utility_list[i][1] + itemset_utility_list[i][2]

    return sum_uo_ro/len(database)

def HUOP_Search (previous_itemset, potential_itemset, beta):
    # print from last line
    # print(f"\r {iteration} {previous_itemset}")

    HUOP_list = []

    # str_list.append(previous_itemset)

    for index, item in enumerate(potential_itemset):
        if futable[item][0] >= beta:
            HUOP_list.append(item)
        upperbound = upperBoundCalculator(item)

        newpotential_itemset = []
        if upperbound >= beta:
            for pitem in potential_itemset[index+1:]:
                # UOlist update
                str_ = UtilityOccupancyTableUpdater(previous_itemset, item, pitem)
                str_list.append(str_)
                newpotential_itemset.append(str_)

        results = HUOP_Search(item, newpotential_itemset, beta)
        HUOP_list += results

    return HUOP_list

def removeDuplicates (list_):
    """
    The function takes in a list and returns a list with the duplicates removed.
    
    :param list_: the list to remove duplicates from
    :return: the list with the duplicates removed
    """
    return list(set(list_))

def window_sliding (database, window_size, window_shift_size, beta):
    """
    The function takes in a database and returns a list of databases with the sliding window applied.
    
    :param database: the database to apply the sliding window to
    :param window_size: the size of the window
    :param window_shift_size: the number of rows to shift the window by
    :return: the list of databases with the sliding window applied
    """
    results = UtilityOccupancyTableGeneration(database, list_of_items)
    # update global uotable and futable
    global uotable
    global futable
    uotable = results[0]
    futable = results[1]

    results = HUOP_Search("", list_of_items, beta)
    return results

def FHM_database_generator (list_results):
    with open("results.txt", "a") as writer:
        for row in list_results:
            mapper = {}
            for item in row:
                for character in item:
                    if character not in mapper:
                        mapper[character] = 1
                    else:
                        mapper[character] += 1
            
            string_name = ""
            mapper_sum = 0
            string_utility = ""

            for key in mapper:
                string_name += str(ord(key) - 96) + " "
                mapper_sum += mapper[key]
                string_utility += str(mapper[key]) + " "
            
            if mapper_sum == 0:
                continue
            
            writer.write(string_name+":"+ str(mapper_sum) +":"+string_utility + "\n")

if __name__ == '__main__':
    beta = float(input("Enter beta: "))
    database = read_database()

    # pprint(database)

    list_of_items = database[0]["itemSet"]

    start_time = time.time()
    print("Calculating...")

    ############################################ WINDOW SLIDING ############################################
    # sliding window
    window_size = 20
    window_shift_size = 5

    i = 0
    j = i + window_size

    # TODO: replace i with j
    while j < len(database):
        iteration += 1
        list_results.append(window_sliding(database[i:j+1], window_size, window_shift_size, beta))

        # shifting the database
        i += window_shift_size
        j += window_shift_size
    
    # pprint(list_results)

    FHM_database_generator(list_results)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Time taken: {elapsed_time}")

    # write in file
    with open('output.json', 'w') as f:
        pretty_list = pformat(list_results)
        f.write(pretty_list)

    ############################################ WINDOW SLIDING ############################################

    # results = UtilityOccupancyTableGeneration(database, list_of_items)
    # uotable = results[0]
    # futable = results[1]

    # results = HUOP_Search("", list_of_items, beta)
    # # pprint(uotable)
    # # pprint(futable)
    # pprint(results)

    # print(f'POSSIBLE COMB => \t\t {len(futable.keys())}')
    # print(f'RESULTS COUNT => \t\t {len(results)}')
    # print(f'CANDIDATE COUNT => \t\t{len(str_list)}')
