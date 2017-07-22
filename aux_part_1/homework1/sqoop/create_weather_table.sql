CREATE DATABASE weatherdb;
USE weatherdb;

CREATE TABLE weather(
	stateId VARCHAR(50),
	date DATE,
	tmin MEDIUMINT,
	tmax MEDIUMINT,
	snow TEXT,
	snwd MEDIUMINT,
	prcp MEDIUMINT
);