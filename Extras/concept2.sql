CREATE SCHEMA IF NOT EXISTS dormitory;


CREATE TABLE dormitory.room_assignments (
assignment_id VARCHAR(20),
student_id INTEGER,
room_number VARCHAR(10),
building_name VARCHAR(100),
semester VARCHAR(10),
monthly_rent DECIMAL(8,2),
assignment_date DATE DEFAULT CURRENT_DATE,
assignment_status VARCHAR(20) DEFAULT 'ACTIVE',
PRIMARY KEY (assignment_id, student_id)
);


CREATE TABLE dormitory.room_assignments_staging (
assignment_id VARCHAR(20),
student_id INTEGER,
room_number VARCHAR(10),
building_name VARCHAR(100),
semester VARCHAR(10),
monthly_rent DECIMAL(8,2),
housing_system VARCHAR(50),
loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO dormitory.room_assignments_staging
(assignment_id, student_id, room_number, building_name, semester, monthly_rent, housing_system) VALUES
('ASG001', 3001, '201A', 'Lumbini', 'FALL2024', 850.00, 'ONLINE_PORTAL'),
('ASG002', 3002, '305B', 'Pokhara', 'FALL2024', 950.00, 'ADMIN_SYSTEM'),
('ASG001', 3003, '201B', 'Lumbini', 'FALL2024', 850.00, 'ONLINE_PORTAL'),
('ASG003', 3004, '102C', 'Mustang', 'FALL2024', 750.00, 'WALK_IN');


INSERT INTO dormitory.room_assignments
(assignment_id, student_id, room_number, building_name, semester, monthly_rent)
SELECT
assignment_id,
student_id,
room_number,
building_name,
semester,
monthly_rent
FROM dormitory.room_assignments_staging
ON CONFLICT (assignment_id, student_id) DO UPDATE SET
room_number = EXCLUDED.room_number,
monthly_rent = EXCLUDED.monthly_rent,
assignment_status = 'UPDATED';

