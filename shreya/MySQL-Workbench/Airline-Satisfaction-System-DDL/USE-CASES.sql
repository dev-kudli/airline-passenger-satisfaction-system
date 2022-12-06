#USE CASES

#1 Count all aircrafts owned by airlines which have a seat capacity>150
SELECT t1.airline_name, t2.aircraft_name, count(t2.aircraft_name) as airline_count, t2.capacity
FROM airline t1
JOIN flights t3 
ON t1.iata_airline = t3.iata_airline
JOIN aircraft t2 
ON t2.aircraft_iata = t3.aircraft_iata
WHERE t2.capacity>150
GROUP by t1.airline_name, t2.aircraft_name, t2.capacity;

#2 Display airline names and the number of positive recommendations receieved 
SELECT t1.airline_name, COUNT(t2.recommended) no_of_recommendation
FROM airline t1
JOIN airline_reviews t2
ON (t1.iata_airline = t2.iata_airline)
WHERE t2.recommended = True
GROUP BY t1.airline_name
ORDER BY no_of_recommendation DESC ;

#3 How many passenger flights do Air France Airlines own?
SELECT t2.airline_name, count(t2.airline_name) AS flight_count
FROM flights t1
INNER JOIN airline t2 
ON t1.iata_airline=t2.iata_airline
INNER JOIN aircraft t3 
ON t1.aircraft_iata=t3.aircraft_iata
WHERE t3.capacity>0 AND t1.iata_airline="AF";

#4 Which airline received maximum average seating comfort rating ?
SELECT t1.airline_name, AVG(t2.seat_comfort_rating) AS avg_seat_comfort_rating
FROM airline t1
JOIN airline_reviews t2
ON (t1.iata_airline = t2.iata_airline)
GROUP BY t1.airline_name
limit 1
;
						
#5 Find all airlines owning  2 passenger flights
SELECT t2.airline_name, count(t2.airline_name) AS airline_count
FROM flights t1
INNER JOIN airline t2 
ON t1.iata_airline=t2.iata_airline
INNER JOIN aircraft t3 
ON t1.aircraft_iata=t3.aircraft_iata
WHERE t3.capacity>0 
GROUP BY t2.airline_name
HAVING count(t2.airline_name)=2;

#6 How many reviews does Air France have with >4.0 rating
SELECT 
t1.iata_airline, t1.review_title, t1.overall_score 
FROM airline_reviews t1
INNER JOIN (SELECT * 
			FROM airline 
            WHERE airline_name like '%air france%' 
            LIMIT 1) as t2 
ON t1.iata_airline=t2.iata_airline
WHERE t1.overall_score>8;

#7 Find top 5 airlines with best overall reviews
SELECT t2.airline_name, AVG(t1.overall_score) as avg_overall_score
FROM airline_reviews t1
INNER JOIN airline t2 
ON t1.iata_airline=t2.iata_airline
GROUP BY t2.airline_name;

#8 Find the airline which provides the best entertainment in flights along with their reviews
                                        
SELECT t2.airline_name,t1.review_title, MAX(avg_rating) as max_average_rating  FROM 
						(SELECT review_title,iata_airline,AVG(entertainment_rating) AS avg_rating
						 FROM airline_reviews
						 GROUP BY iata_airline,review_title ) AS t1 
JOIN airline t2
ON t1.iata_airline=t2.iata_airline
GROUP BY t2.airline_name,t1.review_title
ORDER BY max_rating DESC
limit 1;

#9 order airline names based on the count of their service rating for service rating > 4
SELECT t1.airline_name, COUNT(t2.service_rating) AS service_rating_count
FROM airline t1
JOIN airline_reviews t2
USING(iata_airline)
WHERE t2.service_rating > 4
GROUP BY t1.airline_name
ORDER BY service_rating_count DESC;

#10 Count the number of aircraft each flight has
SELECT t1.flight_number, COUNT(t2.aircraft_name) AS aircraft_count
FROM flights t1
JOIN aircraft t2
ON t1.aircraft_iata = t2.aircraft_iata
GROUP BY t1.flight_number;

#11 


#12

