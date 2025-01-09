#A
SELECT SUM(rentertripstaken) AS total_alquileres
FROM car_rental_db.car_rental_analytics
WHERE fueltype IN ('hybrid', 'electric') AND rating >= 4;

#B
SELECT state_name, SUM(rentertripstaken) AS total_alquileres
FROM car_rental_db.car_rental_analytics
GROUP BY state_name
ORDER BY total_alquileres ASC
LIMIT 5;

#C
SELECT make, model, SUM(rentertripstaken) AS total_alquileres
FROM car_rental_db.car_rental_analytics
GROUP BY make, model
ORDER BY total_alquileres DESC
LIMIT 10;

#D 
SELECT year, SUM(renterTripsTaken) AS total_rentals
FROM car_rental_db.car_rental_analytics
WHERE year BETWEEN 2010 AND 2015
GROUP BY year
ORDER BY year;

#E 
SELECT city, SUM(renterTripsTaken) AS total_rentals
FROM car_rental_db.car_rental_analytics
WHERE fuelType IN ('hybrid', 'electric')
GROUP BY city
ORDER BY total_rentals DESC
LIMIT 5;

#F 
SELECT fuelType, AVG(reviewCount) AS average_reviews
FROM car_rental_db.car_rental_analytics
GROUP BY fuelType
ORDER BY average_reviews DESC;