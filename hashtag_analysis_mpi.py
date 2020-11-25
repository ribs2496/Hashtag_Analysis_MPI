import json
import pandas as pd
import operator
from mpi4py import MPI

def getDict(Data):
    dictionary = {}
    for x in Data:
        if x in dictionary:
            dictionary[x]+=1
        else:
            dictionary[x] = 1
    return dictionary

def printSorted(Data):
    SortedDict = sorted(Data.items(), key=operator.itemgetter(1), reverse = True)
    for i in range(0,10):
        try:
            print(SortedDict[i])
        except:
            print("(Unknown Hashtag , " + str(SortedDict[i][1])+")")
            pass

def codeMap(Data):
    for i in range(0,len(Data)):
        x = Data[i] 
        if x not in Codes:
            continue
        Data[i] = Codes[x]      
    return Data

Codes={"und":"Undefined","am":"Amharic","hu":"Hungarian","pt":"Portuguese","ar":"Arabic","is":"Icelandic","ro":"Romanian","hy":"Armenian","in":"Indonesian","ru":"Russian","bn":"Bengali","it":"Italian","sr":"Serbian","bg":"Bulgarian","ja":"Japanese","sd":"Sindhi","my":"Burmese","kn":"Kannada","si":"Sinhala","zh":"Chinese","km":"Khmer","sk":"Slovak","cs":"Czech","ko":"Korean","sl":"Slovenian","da":"Danish","lo":"Lao","ckb":"Sorani Kurdish","nl":"Dutch","lv":"Latvian","es":"Spanish","en":"English","lt":"Lithuanian","sv":"Swedish","et":"Estonian","ml":"Malayalam","tl":"Tagalog","fi":"Finnish","dv":"Maldivian","ta":"Tamil","fr":"French","mr":"Marathi","te":"Telugu","ka":"Georgian","ne":"Nepali","th":"Thai","de":"German","no":"Norwegian","bo":"Tibetan","el":"Greek","or":"Oriya","tr":"Turkish","gu":"Gujarati","pa":"Panjabi","uk":"Ukrainian","ht":"Haitian","ps":"Pashto","ur":"Urdu","iw":"Hebrew","fa":"Persian","ug":"Uyghur","hi":"Hindi","pl":"Polish","vi":"Vietnamese","cy":"Welsh"}

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
with open('bigTwitter.json',"r",encoding="utf8") as rf:
    lines2read = {k: 1 for k in list(range(rank, 30000000, size))}  
    hashtags = []
    countryCodes = []
    for ei, x in enumerate(rf):
        if ei in lines2read:
            try:
                js = x.strip()[:-1]
                y = json.loads(js)
                countryCodes.append(y["doc"]["lang"])
                for item in y["doc"]['entities']['hashtags']:
                    hashtags.append("#"+item['text'])    
            except:
                pass

countryCodes = codeMap(countryCodes)
hashtags = getDict(hashtags)
countryCodes = getDict(countryCodes)


if rank != 0:
    comm.send([hashtags,countryCodes], dest=0)

if rank == 0:
    data = list(range(size))
    for i in range(size):
        if i != 0:
            data[i] = comm.recv(source=i)   
    data[0] = [hashtags, countryCodes]
    HashDict = {}
    LangDict = {}
    for i in range(size):
        for key, value in data[i][0].items():
            if key in HashDict:
                HashDict[key]+=value
            else:
                HashDict[key] = value

        for key, value in data[i][1].items():
            if key in LangDict:
                LangDict[key]+=value
            else:
                LangDict[key] = value


    print("Top 10 Hashtags:")
    printSorted(HashDict)
    print("\n")
    print("Top 10 Languages used:")
    printSorted(LangDict)

