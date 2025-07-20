CREATE SCHEMA IF NOT EXISTS scholarship;


CREATE TABLE scholarship.student_awards (
student_id INTEGER,
award_date DATE,
scholarship_name VARCHAR(200),
award_amount DECIMAL(10,2),
funding_source VARCHAR(100),
PRIMARY KEY (student_id, award_date, scholarship_name)
);

CREATE TABLE scholarship.student_awards_staging (
student_id INTEGER,
award_date DATE,
scholarship_name VARCHAR(200),
award_amount DECIMAL(10,2),
funding_source VARCHAR(100),
loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO scholarship.student_awards_staging VALUES
(501, '2024-01-15', 'AAA',1500.00, 'University Fund', '2024-01-15 09:00:00'),
(501, '2024-01-15', 'AAA',1500.00, 'University Fund', '2024-01-15 10:30:00'),
(502, '2024-01-15', 'Need-Based Grant', 3000.00, 'State Grant', '2024-01-15 09:15:00'),
(503, '2024-01-15', 'Research Scholarship', 1500.00, 'Alumni Association', '2024-01-15 09:30:00'),
(502, '2024-01-15', 'Need-Based Grant', 3200.00, 'State Grant', '2024-01-15 11:00:00');


INSERT INTO scholarship.student_awards (student_id, award_date, scholarship_name, award_amount, funding_source)
SELECT DISTINCT ON (student_id, award_date, scholarship_name)
student_id,
award_date,
scholarship_name,
award_amount,
funding_source
FROM scholarship.student_awards_staging
ORDER BY student_id, award_date, scholarship_name, loaded_at DESC
ON CONFLICT (student_id, award_date, scholarship_name) DO NOTHING;


