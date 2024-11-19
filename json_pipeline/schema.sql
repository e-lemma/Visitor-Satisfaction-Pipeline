-- This file contains all code required to create & seed database tables.
DROP TABLE IF EXISTS exhibition CASCADE;
DROP TABLE IF EXISTS rating CASCADE;
DROP TABLE IF EXISTS assistance_request CASCADE;
DROP TABLE IF EXISTS assistance CASCADE;
DROP TABLE IF EXISTS floor CASCADE;
DROP TABLE IF EXISTS department CASCADE;
DROP TABLE IF EXISTS review CASCADE;


CREATE TABLE rating(
    rating_id INT GENERATED ALWAYS AS IDENTITY,
    numeric_value INT UNIQUE NOT NULL,
    description TEXT UNIQUE NOT NULL,
    PRIMARY KEY(rating_id),
    CONSTRAINT valid_rating_numeric_value CHECK (numeric_value IN (0,1,2,3,4))
);

CREATE TABLE assistance(
    assistance_id INT GENERATED ALWAYS AS IDENTITY,
    numeric_value INT NOT NULL,
    description TEXT UNIQUE NOT NULL,
    PRIMARY KEY(assistance_id),
    CONSTRAINT valid_assistance_numeric_value CHECK (numeric_value IN (0,1))
);

CREATE TABLE floor(
    floor_id INT GENERATED ALWAYS AS IDENTITY,
    floor_name TEXT UNIQUE NOT NULL,
    PRIMARY KEY(floor_id)
);

CREATE TABLE department(
    department_id INT GENERATED ALWAYS AS IDENTITY,
    department_name TEXT UNIQUE NOT NULL,
    PRIMARY KEY(department_id)
);

CREATE TABLE exhibition(
    exhibition_id INT NOT NULL,
    exhibition_name TEXT UNIQUE NOT NULL,
    description TEXT UNIQUE NOT NULL,
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    floor_id INT NOT NULL,
    department_id INT NOT NULL,
    PRIMARY KEY(exhibition_id),
    FOREIGN KEY(floor_id) REFERENCES floor(floor_id),
    FOREIGN KEY(department_id) REFERENCES department(department_id),
    CONSTRAINT valid_start_date CHECK (start_date <= CURRENT_DATE)
);

CREATE TABLE review(
    review_id INT GENERATED ALWAYS AS IDENTITY,
    exhibition_id INT NOT NULL,
    rating_id INT NOT NULL,
    creation_date TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(review_id),
    FOREIGN KEY(rating_id) REFERENCES rating(rating_id),
    FOREIGN KEY(exhibition_id) REFERENCES exhibition(exhibition_id),
    CONSTRAINT valid_creation_date CHECK (creation_date <= CURRENT_TIMESTAMP + INTERVAL '2 seconds'),
    CONSTRAINT duplicate_reviews UNIQUE (exhibition_id, creation_date)
);

CREATE TABLE assistance_request(
    assistance_request_id INT GENERATED ALWAYS AS IDENTITY,
    exhibition_id INT NOT NULL,
    assistance_id INT NOT NULL,
    creation_date TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(assistance_request_id),
    FOREIGN KEY(exhibition_id) REFERENCES exhibition(exhibition_id),
    FOREIGN KEY(assistance_id) REFERENCES assistance(assistance_id),
    CONSTRAINT valid_creation_date CHECK (creation_date <= CURRENT_TIMESTAMP + INTERVAL '2 seconds'),
    CONSTRAINT duplicate_reqs UNIQUE (exhibition_id, creation_date)
);

INSERT INTO rating(numeric_value, description)
VALUES
    (0, 'Terrible'),
    (1, 'Bad'),
    (2, 'Neutral'),
    (3, 'Good'),
    (4, 'Amazing');
INSERT INTO assistance(numeric_value, description)
VALUES
    (0, 'Assistance'),
    (1, 'Emergency');
INSERT INTO floor(floor_name)
VALUES
    ('1'),
    ('2'),
    ('3'),
    ('vault');
INSERT INTO department(department_name)
VALUES
    ('Geology'),
    ('Entomology'),
    ('Paleontology'),
    ('Zoology'),
    ('Ecology');
INSERT INTO exhibition(exhibition_id, exhibition_name, description, start_date, floor_id, department_id)
VALUES
    (0, 'Measureless to Man', 'An immersive 3D experience: delve deep into a previously-inaccessible cave system.',
    '2021-06-23', 1, 1),
    (1, 'Adaptation', 'How insect evolution has kept pace with an industrialised world.',
    '2019-07-01', 4, 2),
    (2, 'The Crenshaw Collection', 'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.',
    '2021-03-03', 2, 4),
    (3, 'Cetacean Sensations', 'Whales: from ancient myth to critically endangered.',
    '2019-07-01', 1, 4),
    (4, 'Our Polluted World', 'A hard-hitting exploration of humanity''s impact on the environment.',
    '2021-05-12', 3, 5),
    (5, 'Thunder Lizards', 'How new research is making scientists rethink what dinosaurs really looked like.',
    '2023-02-01', 1, 3);