#Borro todos los archivos que puedan haber en la carpeta landing
rm -f /home/hadoop/landing/*.*

#Descargo los archivos en la carpeta landing
wget -O /home/hadoop/landing/car_rental.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv

wget -O /home/hadoop/landing/georef_usa.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

#Borro los archivos presentes en HDFS /ingest
/home/hadoop/hadoop/bin/hdfs dfs -rm /ingest/*.*

#Muevo los archivos de landing a HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/*.* /ingest