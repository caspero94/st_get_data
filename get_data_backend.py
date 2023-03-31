import ccxt
import pymongo
import datetime

# Configuración de la conexión con Binance
ex = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})

# Parámetros de la consulta
symbol = ['BTC/BUSD','ETH/BUSD']
timeframe = ['1M','1w','3d','1d','12h','8h','6h','4h','2h','1h','30m','15m','5m','3m','1m']

# Configuración de la conexión con MongoDB
username = "casper"
password = "caspero"
cluster = "ClusterCrypto"
client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@{cluster}.6ydpkxh.mongodb.net/?retryWrites=true&w=majority")
db = client["CryptoData"]
while True:
    for sym in symbol:
        for tim in timeframe:
            
            # Establecemos base de datos y reset variables buble
            collection = db[sym+"-"+tim]
            last_record = None
            ohlcv = []
            
            # Iniciamos buble recolección
            while True: 
                # Buscamos ultimo registro disponible y sino establecemos fecha de inicio
                last_record = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
                if last_record is not None:
                    from_ts = last_record['_id']
                else:
                    from_ts = ex.parse8601('2017-01-01 00:00:00')
            
                # Desde donde estamos obteniendo la data
                dt = datetime.datetime.fromtimestamp(from_ts / 1000)
                print("Obteniendo datos de "+sym+"-"+tim+" desde "+str(dt))
                
                # Obtención datos historicos
                ohlcv = ex.fetch_ohlcv(sym, tim, since=from_ts, limit=1000)
                datos_ohlcv = [{"_id": i, "open": o, "high": h, "low": l, "close": c, "volume": v} for i, o, h, l, c, v in ohlcv]
            
                # Actualizar o insertar los datos en MongoDB
                updates = []
                for doc in datos_ohlcv:
                    update_doc = {"$set": doc}
                    update_filter = {"_id": doc["_id"]}
                    updates.append(pymongo.UpdateOne(update_filter, update_doc, upsert=True))
                collection.bulk_write(updates)
                
                # Rompemos buble si los datos obtenidos son menor a 1000
                if len(ohlcv)!=1000:
                	break
