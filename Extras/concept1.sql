CREATE SCHEMA IF NOT EXISTS library;

CREATE TABLE library.book_inventory (
book_id VARCHAR(20) PRIMARY KEY,
title VARCHAR(200) NOT NULL,
author VARCHAR(100),
isbn VARCHAR(20),
copies_available INTEGER DEFAULT 0,
last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO library.book_inventory (book_id, title, author, isbn, copies_available) VALUES
('BK001', 'The Great Gatsby', 'F. Scott Fitzgerald', '9780743273565', 5),
('BK002', 'To Kill a Mockingbird', 'Harper Lee', '9780061120084', 3),
('BK003', '1984', 'George Orwell', '9780451524935', 7);


INSERT INTO library.book_inventory (book_id, title, author, isbn, copies_available)
VALUES
('BK002', 'To Kill a Mockingbird', 'Harper Lee', '9780061120084', 8), -- Update existing
('BK004', 'Pride and Prejudice', 'Jane Austen', '9780141439518', 4), -- New book
('BK005', 'The Catcher in the Rye', 'J.D. Salinger', '9780316769174', 6) -- New book
ON CONFLICT (book_id) DO UPDATE SET
copies_available = EXCLUDED.copies_available,
last_updated = CURRENT_TIMESTAMP;




