

import psycopg2
conn = psycopg2.connect(dbname='netology_db', user='postgres', password='2781', host='localhost')
cur = conn.cursor()


def create_db():

    cur.execute("""CREATE TABLE student (
        id serial PRIMARY KEY,
        name varchar(100),
        gpa numeric(10, 2),
        birth timestamp with time zone);
        """)

    cur.execute("""CREATE TABLE course (
        id serial PRIMARY KEY,
        name varchar(100));
        """)

    cur.execute("""CREATE TABLE student_course (
        id serial PRIMARY KEY,
        student_id INTEGER REFERENCES student(id),
        course_id INTEGER REFERENCES  course(id));
        """)

    conn.commit()


def get_students(course_id): # возвращает студентов определенного курса
    cur.execute("""SELECT s.id, s.name, c.name
        FROM student_course sc
        JOIN student s on s.id = sc.student_id
        JOIN course c on c.id = sc.course_id
        WHERE c.id = (%s)
        """, (course_id,))
    return cur.fetchall()


def add_students(course_id, students):
    # создает!(не ищет имеющихся) студентов и записывает их на курс
    for student in students:
        student_id = add_student(student)
        cur.execute(""" INSERT INTO student_course (student_id, course_id) values (%s, %s)
        """, (student_id, course_id, ))
    conn.commit()


def add_student(student):
    # просто создает студента
    cur.execute(""" INSERT INTO student (name, gpa, birth)
        VALUES (%s, %s, %s);
        """, (student["name"], student["gpa"], student["birth"], ))
    cur.execute("""SELECT * FROM student ORDER BY id DESC LIMIT 1;""")
    conn.commit()
    last_added_student = cur.fetchone()
    # возвращает id только что созданного студента
    return last_added_student[0]


def add_course(course):
    cur.execute("""INSERT INTO course(name) VALUES (%s)""", (course["name"], ))
    conn.commit()


def get_student(student_id):
    # выбрать связь студент-курс и присоединить поля
    cur.execute("""SELECT s.id, s.name, c.name
        FROM student_course sc
        JOIN student s on s.id = sc.student_id
        JOIN course c on c.id = sc.course_id
        WHERE s.id = (%s)
        """, (student_id,))
    return cur.fetchall()


if __name__ == '__main__':
    # create_db()

    course = {"name": 'Web'}

    # add_course(course)

    print('выборка таблица course:')
    cur.execute("""SELECT * FROM course""")
    qwe = cur.fetchall()
    print(qwe)

    students = [
        {
            "name": 'Иванов Иван',
            "gpa": 4.53,
            "birth": '2003-08-07'
        },
        {
            "name": 'Петров Петр',
            "gpa": 4.12,
            "birth": '2003-12-05'
        }
    ]

    # add_students(1, students)

    print('выборка таблица student:')
    cur.execute("""SELECT id, name FROM student""")
    qwe = cur.fetchall()
    print(qwe)

    print('данные курса 1 Python:')
    print(get_students(1))

    print('данные студента 1:')
    print(get_student(1))

