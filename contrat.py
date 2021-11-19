import json
import pandas as pd
import random
from datetime import datetime
import pymysql

connection = pymysql.Connect(host='vmi334550.contaboserver.net', port=3307, user='ryenon', password='YENON@2022', database='ryenon')
cursor = connection.cursor()

#Chargement des données
data = [json.loads(line) for line in open('/home/cassandra/Documents/topic/contrats.json', 'r')]

#Initialisation des champs
designation = []
properties = []
tenant = []
montant = []
charges = []
montant_charge = []
dateDebut =[]
dateFin = []
owner = []

#recupération des différents champs
for i in data:
    for k in i['fullDocument']:
        if k == 'designation':
            designation.append(i['fullDocument'].get(k))
        if k == 'propertyId':
            properties.append(i['fullDocument'].get(k))
        if k == 'tenantId':
            tenant.append(i['fullDocument'].get(k))
        if k == 'amount':
            montant.append(i['fullDocument'].get(k))
        if k == 'charges':
            charges.append(i['fullDocument'].get(k).get('amountCharge'))
            montant_charge.append(i['fullDocument'].get(k).get('label'))
        if k == 'startDate':
            dateDebut.append(i['fullDocument'].get(k).get('$date'))
        if k == 'ownerId':
            owner.append(i['fullDocument'].get(k))
        else:
            pass

#Formattage de la colonne dateDebut
tmD = []
for i in range(len(dateDebut)):
    timest = datetime.fromtimestamp(dateDebut/1e3)
    x = timest.strftime('§d/%m/%Y')
    tmD.append(x)

#Formattage des colonnes tenant, owner, montant
tn = random.sample(tenant, 6)
tenant.extend(tn)

ow = random.sample(owner, 6)
owner.extend(ow)

mt = random.sample(montant, 14)
montant.extend(mt)
mt = random.sample(montant, 3)
montant.extend(mt)

#Formattage de la colonne montant_charge
for index, value in enumerate(montant_charge):
    if value == None:
        montant_charge[index] = 0

records_to_insert = []
for i in  range(32):
    records = (designation[i], properties[i], tenant[i], montant[i], charges[i], montant_charge[i], tmD[i], owner[i])
    records_to_insert.append(records)

query = "INSERT INTO contrat (designation, properties, tenant, montant, charge, amountCharge, startDate, owners) VALUES " + ",".join("(%s, %s, %s, %s, %s, %s, %s, %s)" for _ in records_to_insert)
flat_values = [item for sublist in records_to_insert for item in sublist]
cursor.execute(query, flat_values)
connection.commit()
connection.close()
