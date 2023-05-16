# Pablo Jorge Perez

# Proyecto Individual N° 1  
 

Decidi hacer el ETL con Pyspark en lugar de Pandas. Por las siguientes razones:
**Escalabilidad:** PySpark puede manejar grandes volúmenes de datos distribuidos a través de múltiples nodos. Esto significa que puede procesar conjuntos de datos más grandes que los que Pandas podría manejar en una sola máquina.

**Velocidad:** PySpark es más rápido que Pandas en situaciones en las que se requiere procesar grandes conjuntos de datos. PySpark aprovecha la distribución y paralelismo para procesar datos en diferentes nodos de un clúster, lo que reduce el tiempo necesario para procesar grandes volúmenes de datos.

**Procesamiento de datos en tiempo real:** PySpark se puede utilizar en combinación con Apache Kafka o Apache Flume para procesar datos en tiempo real, lo que lo hace una buena opción para aplicaciones de streaming.

De dicho proceso de ETL salen cuatro archivos que se utilizan en la API. Tales son:

`movie.csv`: utilizado en consulta peliculas_mes, peliculas_dia, franquicia y pelicula.
`country.csv`: : utilizado en consulta peliculas_pais
`production.csv`: utilizado en consulta productoras.
`ML.csv`: utilizado en la consulta recomendacion.

Herramientas utilizadas:

* pandas
* pyspark
* uvicorn
* render
* fastapi

El sevicio web fue subido con render el codigo de API fue hecho tanto con Pyspark como Pandas. Esta subido con Pandas aunque está el codigo en Pyspark en el repositorio.

Modo de uso de la API

Para el correcto funcionamiento de la API se debe considerar los siguiente:

    Pegar el código correspondiente a cada consulta a continuación de esta URL.
    Modificar el valor del parámetro introduciendo valores válidos que se encuentren en el Dataset.
    Respetar siempre la ubicación que cada parámetro como se provee en el código.
    El valor a modificar es el valor entre llaves por ejemplo {month}.
    Consulta 1: /mes/{month}
    {month}--> mes :string, donde se informará la cantidad de peliculas estrenadas en ese mes. Por ejemplo: /mes/Enero.
    Consulta 2: /dia/{dia}
    {dia}--> dia :string, donde se informará la cantidad de peliculas estrenadas en ese mes. Por ejemplo: /dia/Jueves.
    Consulta 3: /franquicia/{franquicia}
    {franquicia}--> nombre de la coleccion :string, donde se informará el nombre de la coleecion, la cantidad de peliculas ue forma parte, la ganacia total y la ganacia promedio de la colleccion. Por ejemplo: /franquicia/Toy Story Collection.
    Consulta 4: /pais/{pais}
    {pais}--> pais:string, donde informará  el país, y la cantidad de peliculas producidas por el mismo. Por ejemplo /pais/United States of America.
    Consulta 5: /productora/{productora}
    {productora}--> nombre de la productora, informando productora, cantidad de peliculas producidas y ganacia Total. Por jemplo /productora/Pixar Animation Studios"

    
    
    

link API: https://jj-9im1.onrender.com
link video: https://youtu.be/wBDOOvbXgXY