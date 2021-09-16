CREATE TABLE IF NOT EXISTS administrators (
  id SERIAL,
  username VARCHAR(45) NOT NULL,
  email VARCHAR(45) NOT NULL,
  password TEXT NOT NULL,
  PRIMARY KEY (id)
);