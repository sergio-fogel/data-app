########## TEST 1 (EXTRACT) ##########

#Debería validar la lógica de extracción

#Para que el test no vaya en serio a hacer el request a la API, mockear la entrada (hacer un mock del get que devuelva una data hardcodeada y status 200)
#Y lo mismo para el lado de la escritura (return df to JSON), así el test no escribe nada realmente en ningún file
#Y validar que todo el proceso del medio corra OK

#https://python-tutorials.in/python-mock-requests/


########## TEST 2 (REPORT) ##########

#Debería testear las funciones que trabajan sobre el resultado que devuelve el SELECT a la base (no sobre el SELECT en sí mismo)
#¡¡¡PERO... No tengo funciones que agreguen (ordenen, agrupen, etc) porque mplfinance resulve estas cuestiones del reporte!!!

#Guardar la respuesta de la base en un json y armar un mock que devuelva eso en formato dataframe

