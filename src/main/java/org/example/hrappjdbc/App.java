package org.example.hrappjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.GregorianCalendar;

import org.example.model.Employee;

/**
 * Hello world!
 *
 */
public class App {
	private static final String DB_URL = "jdbc:mysql://localhost:3306/hr";
	private static final String USER = "root";
	private static final String PASSWORD = "admin";
	
	private static Connection connection;
	
	public static void main(String[] args) {
		try {
			connection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
			String sql = "SELECT employee_id, first_name, last_name, email FROM employees";
			
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			
			while(resultSet.next()) {
				int employeeId = resultSet.getInt("employee_id");
				String firstName = resultSet.getString("first_name");
				String lastName = resultSet.getString("last_name");
				String email = resultSet.getString("email");
						
				System.out.print("EmployeeID: " + employeeId);
				System.out.print(" FirstName: " + firstName);
				System.out.print(" LastName: " + lastName);
				System.out.println(" Email: " + email);
			}
			
			System.out.println("------------ employees wih job title ------------");
			
			sql = "SELECT employee_id, first_name, last_name, job_title FROM employees as e INNER JOIN jobs as j ON e.job_id = j.job_id";
			resultSet = statement.executeQuery(sql);
			
			while(resultSet.next()) {
				int employeeId = resultSet.getInt("employee_id");
				String firstName = resultSet.getString("first_name");
				String lastName = resultSet.getString("last_name");
				String jobTitle = resultSet.getString("job_title");
				
				System.out.print("EmployeeID: " + employeeId);
				System.out.print(" FirstName: " + firstName);
				System.out.print(" LastName: " + lastName);
				System.out.println(" jobTitle: " + jobTitle);
			}
			
			
			System.out.println("------------- employees with job title and stard date ---------------------");
			
			System.out.println("Update employee with id=204 to have job shipping clerk");
			
			sql = "update employees set job_id = 'SH_CLERK' WHERE employee_id=204";
			statement.executeUpdate(sql);
			
			sql = "SELECT employee_id, first_name, last_name, job_title FROM employees as e INNER JOIN jobs as j ON e.job_id = j.job_id WHERE employee_id=204";
			resultSet = statement.executeQuery(sql);
			while(resultSet.next()) {
				int employeeId = resultSet.getInt("employee_id");
				String firstName = resultSet.getString("first_name");
				String lastName = resultSet.getString("last_name");
				String jobTitle = resultSet.getString("job_title");
				
				System.out.print("EmployeeID: " + employeeId);
				System.out.print(" FirstName: " + firstName);
				System.out.print(" LastName: " + lastName);
				System.out.println(" jobTitle: " + jobTitle);
			}
			
			System.out.println("----------- using prepared statement to update and select an employee with job -----------------");
			updateEmployeeJob(204, "AD_PRES");
			getEmployeeById(204);
			
			System.out.println("-------- insert new employee into the database ------------------");
			Employee employee = new Employee(210, "Ramona", "Cristea", "ramocristea1", "1234", new GregorianCalendar(2018, 7, 16).getTime(), "IT_PROG", 10000, 0.54, 204, 60);
			insertEmployee(employee);
			getEmployeeById(210);
			
			System.out.println("------------ operations in transaction starting here -------------------");
			updateEmployeeJob(203, "SH_CLERK");
			getEmployeeById(203);
			updateEmployeeJobAndInsertJobHistory("AC_MGR", 203, new Date(), new Date(), 0);
			getEmployeeById(203);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void updateEmployeeJob(int employeeId, String jobId) {
		String sql = "update employees set job_id = ? WHERE employee_id = ?";
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, jobId);
			preparedStatement.setInt(2, employeeId);
			
			preparedStatement.executeUpdate();
			
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void getEmployeeById(int employeeId) {
		String sql = "SELECT employee_id, first_name, last_name, job_title FROM employees as e INNER JOIN jobs as j ON e.job_id = j.job_id WHERE e.employee_id = ?";
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, employeeId);
			
			ResultSet resultSet = preparedStatement.executeQuery();
			
			while(resultSet.next()) {
				int employeeIdResult = resultSet.getInt("employee_id");
				String firstName = resultSet.getString("first_name");
				String lastName = resultSet.getString("last_name");
				String jobTitle = resultSet.getString("job_title");
				
				System.out.print("EmployeeId: " + employeeIdResult);
				System.out.print(" FirstName: " + firstName);
				System.out.print(" LastName: " + lastName);
				System.out.println(" JobTitle: " + jobTitle);
			}
			
			resultSet.close();
			preparedStatement.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void insertEmployee(Employee employee) {
		String sql = "INSERT into employees(employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, employee.getEmployeeId());
			preparedStatement.setString(2, employee.getFirstName());
			preparedStatement.setString(3, employee.getLastName());
			preparedStatement.setString(4, employee.getEmail());
			preparedStatement.setString(5, employee.getPhoneNumber());
			preparedStatement.setDate(6, new java.sql.Date(employee.getHireDate().getTime()));
			preparedStatement.setString(7, employee.getJobId());
			preparedStatement.setDouble(8, employee.getSalary());
			preparedStatement.setDouble(9, employee.getCommissionPct());
			preparedStatement.setInt(10, employee.getManagerId());
			preparedStatement.setInt(11, employee.getDepartmentId());
			
			preparedStatement.executeUpdate();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void updateEmployeeJobAndInsertJobHistory(String jobId, int employeeId, Date startDate, Date endDate, int departmentId) {
		String updateJobForEmployee = "UPDATE employees set job_id = ? WHERE employee_id = ?";
		String insertJobHistoryForEmployee = "INSERT into job_history(employee_id, start_date, end_date, job_id) VALUES(?,?,?,?)";
		
		try {
			connection.setAutoCommit(false);
			PreparedStatement updateJobForEmployeeStatement = connection.prepareStatement(updateJobForEmployee);
			updateJobForEmployeeStatement.setString(1, jobId);
			updateJobForEmployeeStatement.setInt(2, employeeId);
			
			updateJobForEmployeeStatement.executeUpdate();
			
			PreparedStatement insertJobHistoryStatement = connection.prepareStatement(insertJobHistoryForEmployee);
			insertJobHistoryStatement.setInt(1, employeeId);
			insertJobHistoryStatement.setDate(2, new java.sql.Date(startDate.getTime()));
			insertJobHistoryStatement.setDate(3, new java.sql.Date(endDate.getTime()));
			insertJobHistoryStatement.setString(4, jobId);
			
			insertJobHistoryStatement.executeUpdate();
			
			connection.commit();
			
			updateJobForEmployeeStatement.close();
			insertJobHistoryStatement.close();
			connection.setAutoCommit(true);
			
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				try {
					connection.setAutoCommit(true);
				} catch (SQLException e2) {
					e2.printStackTrace();
				}
			}
			e.printStackTrace();
		}
	}
}
