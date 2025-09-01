cust_to_number = {
            "KL": 14,
            "LM": 13,
            "PGR": 11,
            "SAIS": 9,
            "USAA": 17,
            "NGIC": 24,
            "UNFY": 35,
            "BLAG": 22,
            "LMX": 38,
            "BETA": 36,
            "COMP": 39,
            "KLX": 40,
            "BLAX" : 34
        }

cust = [35,22,34,38,39,40,24,17,36]
arr = []
for c in cust:
    for k,v in cust_to_number.items():
    
        if v == c:
            arr.append(k)
            print(k)
print(arr)
# for k,v in  cust_to_number:
#     print(k+ "" + str(v))
    