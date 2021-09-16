CREATE TABLE IF NOT EXISTS products (
  id SERIAL,
  name VARCHAR(45) NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  quantity INT NULL,
  image VARCHAR(200) NULL,
  image_large VARCHAR(200) NULL,
  category_id INT NULL,
  PRIMARY KEY (id),
  CONSTRAINT category_fk FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE NO ACTION ON UPDATE NO ACTION
);