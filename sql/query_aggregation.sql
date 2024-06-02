SELECT
    COUNT(*) AS total_books,
    AVG(year_published) AS avg_year_published,
    AVG(LENGTH(title)) AS avg_title_length
FROM
    books;