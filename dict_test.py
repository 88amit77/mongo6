data={"name":"amit","age":29}
print(data)
##print value based on key
print(data['name'])

##add new data or update
#method 1
data["phone"]=99999
print(data)
#method 2
data.update({"sex":"male"})
print("updated-->",data)

data["phone"]=99999
print(data)

##delete data
#method=1
del data["sex"]
print("deleted data",data)
#method=2
age=data.pop("age")
print("deleted data",data)
print("value of key data deleted",age)


###print default data if key is not present using get

###method 1
print(data.get("nam1"))

###method 2
print(data.get("nam1","NOT found"))

###loop on dict
#method 1
for key in data:
    print("key-->",key)

#method 2
for i in data.items():
    print("i-->",i)

#method 3
for key,value in data.items():
    print("j-->",key,value)
##how to create dict with diff method
d=dict(job="software",sal=42000)
print(d)

###dict comprehension
d1={1:2,2:3,5:6}
a={i:i**2 for i in range(5) if i%2==0}
print(a)