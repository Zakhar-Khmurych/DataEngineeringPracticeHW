import duckdb

conn = duckdb.connect()
conn.execute("""

CREATE TABLE countries (
    country_name VARCHAR(100) PRIMARY KEY,
    capital VARCHAR(100)
);
CREATE TABLE population (
    country_name VARCHAR(100),
    population INT,
    FOREIGN KEY (country_name) REFERENCES countries(country_name)
);
CREATE TABLE currency (
    country_name VARCHAR(100),
    currency_name VARCHAR(100),
    FOREIGN KEY (country_name) REFERENCES countries(country_name)
);
CREATE TABLE area (
    country_name VARCHAR(100),
    area DECIMAL(30, 2),  -- площа в квадратних кілометрах
    FOREIGN KEY (country_name) REFERENCES countries(country_name)
);
CREATE TABLE government_type (
    country_name VARCHAR(100),
    government_form VARCHAR(100),
    FOREIGN KEY (country_name) REFERENCES countries(country_name)
);


INSERT INTO countries (country_name, capital) VALUES
('Ukraine', 'Kyiv'),
('Germany', 'Berlin'),
('USA', 'Washington, D.C.'),
('France', 'Paris'),
('India', 'New Delhi'),
('Sweden', 'Stockholm'),
('Brazil', 'Brasília'),
('Egypt', 'Cairo'),
('Moldova', 'Chișinău'),
('Laos', 'Vientiane');

INSERT INTO population (country_name, population) VALUES
('Ukraine', 41800000),
('Germany', 83000000),
('USA', 331000000),
('France', 67000000),
('India', 1380004385),
('Sweden', 10327589),
('Brazil', 213993437),
('Egypt', 104124440),
('Moldova', 2657637),
('Laos', 7275560);

INSERT INTO currency (country_name, currency_name) VALUES
('Ukraine', 'Hryvnia'),
('Germany', 'Euro'),
('USA', 'US Dollar'),
('France', 'Euro'),
('India', 'Indian Rupee'),
('Sweden', 'Swedish Krona'),
('Brazil', 'Brazilian Real'),
('Egypt', 'Egyptian Pound'),
('Moldova', 'Moldovan Leu'),
('Laos', 'Laotian Kip');

INSERT INTO area (country_name, area) VALUES
('Ukraine', 603550),
('Germany', 357022),
('USA', 9833520),
('France', 551695),
('India', 3287263),
('Sweden', 450295),
('Brazil', 8515767),
('Egypt', 1002450),
('Moldova', 33846),
('Laos', 237955);

INSERT INTO government_type (country_name, government_form) VALUES
('Ukraine', 'Republic'),
('Germany', 'Federal Republic'),
('USA', 'Federal Republic'),
('France', 'Republic'),
('India', 'Federal Republic'),
('Sweden', 'Constitutional Monarchy'),
('Brazil', 'Federal Republic'),
('Egypt', 'Republic'),
('Moldova', 'Republic'),
('Laos', 'Communist State');

"""
)
result = conn.execute(
    """
    SELECT 
        gov.government_form, c.capital, (p.population / a.area) AS population_density, curr.currency_name
    FROM
        countries c
    JOIN 
        government_type gov ON c.country_name = gov.country_name    
    JOIN 
        currency curr ON c.country_name = curr.country_name
    JOIN
        population p ON c.country_name = p.country_name
    JOIN
        area a ON c.country_name = a.country_name
    WHERE 
        (p.population / a.area) > 100
    ORDER BY 
        gov.government_form, population_density DESC;
    
    """
).fetchall()
print(result)

result2 = conn.execute(
    """
    SELECT 
        gov.government_form, 
        c.capital, 
        (p.population / a.area) AS population_density, 
        curr.currency_name,
        DENSE_RANK() OVER (PARTITION BY gov.government_form ORDER BY (p.population / a.area) DESC) AS population_density_rank
    FROM
        countries c
    JOIN 
        government_type gov ON c.country_name = gov.country_name    
    JOIN 
        currency curr ON c.country_name = curr.country_name
    JOIN
        population p ON c.country_name = p.country_name
    JOIN
        area a ON c.country_name = a.country_name
    WHERE 
        (p.population / a.area) > 100
    ORDER BY 
        gov.government_form, population_density DESC;
    """
).fetchall()
print(result2)

conn.close()
