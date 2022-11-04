CREATE TABLE processed_listings
(
    link    VARCHAR(255) NOT NULL PRIMARY KEY,
    source  VARCHAR(255) NOT NULL,
    name    VARCHAR(255) NOT NULL,
    price   VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL
);