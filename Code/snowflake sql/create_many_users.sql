-- List of usernames and passwords
SET usernames = ARRAY['user1', 'user2', 'user3'];
SET passwords = ARRAY['password1', 'password2', 'password3'];

-- Loop through the usernames and passwords
SET i = 1;
WHILE :i <= ARRAY_SIZE(:usernames)
DO
  -- Generate SQL statement to create user
  SET create_user_sql = 
    'CREATE USER ' || :usernames[:i] || ' PASSWORD = ''' || :passwords[:i] || '''';
    
  -- Execute the SQL statement
  EXECUTE IMMEDIATE :create_user_sql;
  
  -- Increment counter
  SET i = :i + 1;
END WHILE;

