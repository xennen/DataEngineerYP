CREATE VIEW analysis.users AS
    SELECT *
    FROM production.users;

CREATE VIEW analysis.orderitems AS
    SELECT *
    FROM production.orderitems;

CREATE VIEW analysis.orderstatuses AS
    SELECT *
    FROM production.orderstatuses;

CREATE VIEW analysis.products AS
    SELECT *
    FROM production.products;

CREATE VIEW analysis.orders AS
    SELECT *
    FROM production.orders;
