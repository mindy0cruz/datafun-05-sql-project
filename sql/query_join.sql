-- Perform INNER JOIN operation on two tables
SELECT books.title, authors.last_name
FROM books
INNER JOIN authors ON books.author_id = authors.author_id;