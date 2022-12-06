#DDL 
CREATE DATABASE airline_passenger_satisfaction_system;

CREATE TABLE country
(country_code VARCHAR(15) UNIQUE,
country_name VARCHAR(50), 
PRIMARY KEY (country_code));


CREATE TABLE country_area
(area_code_id VARCHAR(255) UNIQUE,
country_code VARCHAR(15),
world_area_code VARCHAR(15),
PRIMARY KEY (area_code_id),
FOREIGN KEY(country_code) REFERENCES COUNTRY(country_code) ON DELETE CASCADE);

CREATE TABLE city
(city_code VARCHAR(15) NOT NULL,
city_name VARCHAR(30), 
country_code VARCHAR(15) NOT NULL,
PRIMARY KEY (city_code),
FOREIGN KEY(country_code) REFERENCES COUNTRY(country_code) ON DELETE CASCADE);

CREATE TABLE airport
(iata_airport VARCHAR(15) UNIQUE,
icao_airport VARCHAR(15),
airport_name VARCHAR(50),
state VARCHAR(50),
latitude float,
longitude float,
area_code_id VARCHAR(255),
PRIMARY KEY (iata_airport),
FOREIGN KEY(area_code_id) REFERENCES country_area(area_code_id),
FOREIGN KEY(iata_airport) REFERENCES city(city_code));

ALTER TABLE AIRPORT
ADD FOREIGN KEY(area_code_id) REFERENCES COUNTRY_AREA(area_code_id);

ALTER TABLE AIRPORT
ADD FOREIGN KEY(iata_airport) REFERENCES CITY(city_code);

CREATE TABLE airline_country_detail
(country_airline_id VARCHAR(255) UNIQUE,
iata_airline VARCHAR(15) UNIQUE,
country_code VARCHAR(15),
PRIMARY KEY (country_airline_id),
FOREIGN KEY(country_code) REFERENCES country(country_code));

CREATE TABLE airline
(iata_airline VARCHAR(15) UNIQUE,
icao_airline VARCHAR(15),
airline_name VARCHAR(100),
active VARCHAR(50),
PRIMARY KEY (iata_airline),
FOREIGN KEY(iata_airline) REFERENCES airline_country_detail(iata_airline));

ALTER TABLE AIRLINES
ADD FOREIGN KEY(iata_airline) REFERENCES AIRLINE_COUNTRY_DETAILS(iata_airline);

CREATE TABLE airline_reviews
(airline_review_id INT NOT NULL AUTO_INCREMENT,
iata_airline VARCHAR(15) UNIQUE,
review_title VARCHAR(255),
review_date DATE,
seat_comfort_rating INT,
service_rating INT,
food_rating INT,
entertainment_rating INT,
groundservice_rating INT,
wifi_rating INT,
value_rating INT,
overall_score INT,
recommended INT,
PRIMARY KEY (airline_review_id),
FOREIGN KEY(iata_airline) REFERENCES airline(iata_airline));

CREATE TABLE IF NOT EXISTS aircraft(
aircraft_iata CHAR(3),
aircraft_icao CHAR(5),
aircraft_name VARCHAR(50),
capacity int,
PRIMARY KEY (aircraft_iata)
);

CREATE TABLE IF NOT EXISTS flights(
flight_id INT AUTO_INCREMENT,
aircraft_iata CHAR(3),
flight_number INT,
iata_airline VARCHAR(15),
PRIMARY KEY (flight_id),
FOREIGN KEY (aircraft_iata) REFERENCES aircraft(aircraft_iata),
FOREIGN KEY (iata_airline) REFERENCES airline(iata_airline)
);  

#INSERT INTO FLIGHTS
INSERT INTO flights(aircraft_iata,flight_number,iata_airline) values("100",900,"CE");
INSERT INTO flights(aircraft_iata,flight_number,iata_airline) values("340",300,"AF");
INSERT INTO flights(aircraft_iata,flight_number,iata_airline) values("142",300,"AF");
INSERT INTO flights(aircraft_iata,flight_number,iata_airline) values("142",900,"BA");
INSERT INTO flights(aircraft_iata,flight_number,iata_airline) values("340",200,"BA");
INSERT INTO flights(aircraft_iata,flight_number,iata_airline) values("100",800,"CE");




#DROP COMMANDS
DROP TABLE COUNTRY_AREA;
DROP TABLE CITY;
DROP TABLE AIRPORT;
DROP TABLE AIRLINE_COUNTRY_DETAIL;
DROP TABLE COUNTRY;
DROP TABLE AIRLINE;
DROP TABLE  AIRLINE_REVIEWS;
DROP TABLE AIRCRAFT;
DROP TABLE FLIGHTS;
DROP TABLE AIRLINE_COUNTRY_DETAILS;


