-- The data has been normalized into two tables: Country and WorldHappinessDetails

-- The WorldHappinessDetails table has a combination of 2 primary keys: Year and CountryId (Foreign key constraint with Country table)


CREATE TABLE Country(
	CountryId int NOT NULL AUTO_INCREMENT,
	Name varchar(255),
	PRIMARY KEY(CountryId)
);


CREATE TABLE WorldHappinessDetails(
	Year year NOT NULL,
	CountryId int NOT NULL,
	HappinessScore float,
	StandardError float,
	Family float,
	Economy float,
	LifeExpectancy float,
	Freedom float,
	GovernmentCorruption float,
	Generosity float,
	DystopiaResidual float,
	PRIMARY KEY(Year, CountryId)
	FOREIGN KEY (CountryId) REFERENCES Country (CountryId)
);