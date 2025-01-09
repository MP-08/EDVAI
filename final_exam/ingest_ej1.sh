
#Borro todos los archivos que puedan haber en la carpeta landing / It deletes all the files that might be in the landing folder
rm -f /home/hadoop/landing/*.*

#Descargo los archivos en la carpeta landing / It downloads the files into the landing folder.
wget -O /home/hadoop/landing/2021-informe.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

wget -O /home/hadoop/landing/202206-informe.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv

wget -O /home/hadoop/landing/aeropuertos.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

#Borro los archivos presentes en HDFS /ingest /It deletes the files present in HDFS /ingest.
/home/hadoop/hadoop/bin/hdfs dfs -rm /ingest/*.*

#Muevo los archivos de landing a HDFS / It moves the files from landing to HDFS.
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/*.* /ingest