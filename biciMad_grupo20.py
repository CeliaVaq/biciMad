"""
BiciMad

Lucia Del Nido Herranz
Victor Guejes Cepeda
Celia Vaquero Espinosa

"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,udf,isnull,collect_list, struct
from pyspark.sql.types import IntegerType, StringType, BooleanType


FILES = ["/home/mat/Escritorio/Paralela/biciMad/201801_Usage_Bicimad.json",
         "/home/mat/Escritorio/Paralela/biciMad/201802_Usage_Bicimad.json"]
#Aquí cada uno pone su dirección

def union_archivos():
    rddlst = []
    for f in FILES:
        df = spark.read.json(f)
        df = df.drop('_corrupt_record','track')
        rddlst.append(df)
    df = reduce(DataFrame.unionAll, rddlst)
    return df

def main():
    df = union_archivos()
    # to fit df in page and see more clearly the results
    df = df.drop('_id','unplug_hourTime','idplug_base','idunplug_base','track')
    df.show()
    
    
    ################################# 1 ######################################
    #contamos los usuarios que usan las bicis por cada rango de edad
    #.sort() para que aparezca el numero mas alto arriba
    print('rango de edad que más lo utiliza')
    df.groupBy('ageRange').count().sort('count', ascennding = True).show() #esto no modifica el rdd
    
    
    ################################# 2 ######################################
    print('residencia')
    #nos quedamos con los usuarios del tipo 1 y quitamos los usuarios sin codigo postal
    df1 = df.filter(df["user_type"]==1).filter(df.zip_code != '') #no se quitan bien todos los nulos
    #para que los codigos postales sean enteros
    df1 = df1.withColumn("zip_code",col("zip_code").cast("Integer"))
    
    #agrupamos entre los que son de madrid (zip_code empieza por 28) y los que no
    df1 = df1.withColumn("zip_code",(col("zip_code")-col("zip_code")%1000)==28000).withColumnRenamed ("zip_code", "Madrid_residents")
    df1.groupBy('Madrid_residents').count().sort('count', ascennding = True).show()
    
    
    ################################# 3 ######################################
    print('viajes por usuario')
    #Vamos a ver primero qué viajes realiza un usuario cada día:
    #creamos una lista de listas definidas como [estacion orgigen. estacion llegada]
    df2=df.groupBy('user_day_code').agg(collect_list(struct('idunplug_station','idplug_station')).alias('stations'))
    df2.show()
    
    #Ahora nos interesa saber cuántos viajes realiza en un día cada usuario:
    print('numero de viajes por usuario')
    trips_len = udf(lambda x: len(x), IntegerType())
    dfv = df2.withColumn("stations",trips_len(col("stations"))).withColumnRenamed ("stations", "number_of_trips")
    dfv.show()
    
    print('recuento de usuarios por numero de viajes')
    #Vamos a ver cuántos usuarios han realizado 1 viaje por día, 2 viajes, 3 viajes ... 
    dfv.groupBy('number_of_trips').count().sort('number_of_trips', ascending = False).show()


    ################################# 4 ######################################
    print('viajes de ida y vuelta')
    #Ahora vamos a ver si hay viajes de ida y vuelta:
    round_trip_udf = udf(lambda x: round_trip(x), BooleanType())
    df3 = df2.withColumn('round_trip', round_trip_udf(df2['stations'])).alias('dfgrt')
    df3.show()

    print('recuento de usuarios con viajes de ida y vuelta')
    #Vamos a ver cuántos usuarios han realizado un viaje de ida y vuelta versus los que no:
    df3.groupBy('round_trip').count().sort('round_trip').show()



    ################################# 5 ######################################
    #Vamos a ver la duración de los viajes:
    print('duracion de los viajes')
    dfd = df.drop("ageRange","idplug_station","idunplug_station","user_type","zip_code")

    #Vamos a categorizar la duración de los viajes según han sido cortos, medios o largos: 
    cat_time_udf = udf(lambda x: cat_time(x))
    dfd2 = dfd.withColumn('travel_time_cat',cat_time_udf(dfd['travel_time']))
    dfd2 = dfd2.drop('travel_time')
    dfd2.show()

    #Ahora veamos cuántos viajes se han realizado de cada tipo:
    print('recuento de viajes por duracion')
    dfd2.groupBy('travel_time_cat').count().sort('count').show()


    ################################# 6 ######################################
    #Ahora vamos a ver en qué fechas son se utilizaron más las bicis
    dff = union_archivos()
    dff = dff.drop('_id','ageRange','user_type','zip_code','idplug_base','idunplug_base','track')
    dff.show()


    #Vamos a eliminar la franja horaria porque nos interesan únicamente la fecha:
    read_date_udf = udf(lambda x: read_date(x))  
    dff2 = dff.withColumn('date',read_date_udf(dff['unplug_hourTime']))
    #Vamos a ver durante qué fechas hubo más trayectos en bicicleta.
    print('dias con mas trayectos')
    dff2.groupBy('date').count().sort('count',ascending = False).show()
    
    
    #Hacemos el mismo proceso para ver qué meses ha habido más trayectos:
    get_month_udf = udf(lambda x: get_month(x))  
    month_udf = dff.withColumn('month',get_month_udf(dff['unplug_hourTime']))
    print('meses con mas trayectos')
    month_udf.groupBy('month').count().sort('count',ascending = False).show()
    
    

    ################################# 7 ######################################
    #Calculemos qué estaciones son más concurridas como origen del viaje:
    print('estaciones de salida mas concurridas')
    dff.groupBy('idplug_station').count().sort('count',ascending = False).show()
    #como estacion de llegada
    print('estaciones de llegada mas concurridas')
    dff.groupBy('idunplug_station').count().sort('count',ascending = False).show()


    ################################# 8 ######################################
    #Vemos que rango de edad hace los trayectos de mayor duracion
    print('edades que realizan los viajes mas largos')
    df8 = df.drop("idplug_station","idunplug_station","user_type","zip_code") #,'user_day_code')

    #Categorizamos la duracion de los trayectos y dejamos solo los viajes largos:
    cat_time_udf = udf(lambda x: cat_time(x))
    df8 = df8.withColumn('travel_time',cat_time_udf(df8['travel_time']))
    #quitamos el rango de edad 0 ya que se corresponde con las edades desconocidas
    df_largo = df8.filter(df8["travel_time"]=='Largo').filter(df8["ageRange"]!='0')
    df_largo2 = df_largo.groupBy('ageRange').count().sort('count',ascending = False).withColumnRenamed('count','count_largo')
    df_largo2.show()

    print('edades que realizan los viajes de duracion media')
    df_medio = df8.filter((df8["travel_time"]=='Medio')&(df8["ageRange"]!='0'))
    df_medio2 = df_medio.groupBy('ageRange').count().sort('count',ascending = False).withColumnRenamed('count','count_medio')
    df_medio2.show()

    print('edades que realizan los viajes mas cortos')
    df_corto = df8.filter((df8["travel_time"]=='Corto')&(df8["ageRange"]!='0'))
    df_corto2 = df_corto.groupBy('ageRange').count().sort('count',ascending = False).withColumnRenamed('count','count_corto')
    df_corto2.show()

    df_aux1 = df_medio2.join(df_largo2, on=['ageRange'], how='left_outer')
    df_aux2 = df_corto2.join(df_aux1, on=['ageRange'],how='left_outer')

    df_aux2.sort('ageRange').show()

    
    
    ################################ 9 ########################################
    #Vamos a hacer un estudio por cada rango de edad:
    for i in [1,2,3,4,5]:
        print(f"Vamos a estudiar el rango de edad {i}:")
        est,cod = origen(df,i)
        print(f"La estación de origen que toma más comunmente es: {est} y los usuarios provienen principalmente del código postal {cod}")
        
        est,cod = destino(df,i)
        print(f"La estación de destino que usada más frecuentemente es: {est} y los usuarios provienen principalmente del código postal {cod}")
        print("--------------------------------------------------------------")
    
    
    
    
#Nos dice si el viaje es de ida y vuelta.
def round_trip(x):
    if len(x) != 2: #solo los viajes con longitud 2 pueden ser de ida y vuelta
        return False
    else:
        return x[0][1] == x[1][0] and x[0][0] == x[1][1]



#Cambia la etiqueta de los viajes segun la duración
def cat_time(x):
    result = 'Largo'
    if int(x) <500:
        result = "Corto"
    elif int(x)<1000:
        result = "Medio"
    return result

#Para quedarnos únicamente con la fecha:     
def read_date(x):
    return x[0][0:10]

def get_month(x):
    return x[0][0:7]


def origen(datos,i):
    dfe_aux = datos.filter(datos["ageRange"]==i)

    #Vamos a ver de qué estación salen los usuarios pertenecientes al grupo de edad i: 
    dfe_aux.groupBy('idunplug_station').count().sort('count',ascending = False).show() 
    estacion,count = dfe_aux.groupBy('idunplug_station').count().sort('count',ascending = False).head()  #La estación más usada

    dfe_aux = datos.filter(datos["idunplug_station"]==estacion).filter(datos["zip_code"]!='')
    dfe_aux.groupBy('zip_code').count().sort('count',ascending = False).show()
    codigo, count = dfe_aux.groupBy('zip_code').count().sort('count',ascending = False).head()
    return (estacion,codigo)

def destino(datos, i):
    dfe_aux = datos.filter(datos["ageRange"]==i)

    #Vamos a ver de qué estación salen los usuarios pertenecientes al grupo de edad i: 
    dfe_aux.groupBy('idplug_station').count().sort('count',ascending = False).show() 
    estacion,count = dfe_aux.groupBy('idplug_station').count().sort('count',ascending = False).head()  #La estación más usada

    dfe_aux = datos.filter(datos["idplug_station"]==estacion).filter(datos["zip_code"]!='')
    dfe_aux.groupBy('zip_code').count().sort('count',ascending = False).show()
    codigo, count = dfe_aux.groupBy('zip_code').count().sort('count',ascending = False).head()
    return (estacion,codigo)
   



if __name__ == "__main__":
    main()



"""

1. Qué rango de edad usa más las bicis

2. Distinguir si el código postal es de Madrid ciudad o de fuera
(quiénes usan más las bicis?) teniendo en cuenta el tipo de usuario.

3. Vemos los viajes que realiza cada usuario y cuantos son.

4. Vemos qué viajes son de ida y vuelta y cuántos hay.

5. Repartimos los viajes en distintas catgorías dependiendo de su duración.

6. Calculamos cuántos viajes de hacen cada día y vemos qué día ha sido en el
que se ha usado más biciMAD. Tambien en que mes se usan mas biciMad.

7. Calculamos las estaciones de origen y de llegada más concurrentes

8.

9.


"""
