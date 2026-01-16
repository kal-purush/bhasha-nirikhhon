package Orange.Definitions;

import org.openqa.selenium.WebDriver;

import Orange.Pages.LeavePage;
import Orange.Pages.LoginPage;
import Orange.Pages.PimPage;
import Orange.Steps.Conexion;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;


public class DefinitionsSteps {

	private WebDriver driver;
	private Conexion conexion = new Conexion();
	//llamado a una clase  para poder acceder a los metos
	private LoginPage loginPage = new LoginPage(driver);
	private PimPage pimPage = new PimPage(driver);
	private LeavePage leavePage = new LeavePage(driver);
	
	@Given("^abrir el navegador$")
	public void abrir_navegador() {
		this.conexion = new Conexion();
		this.driver = this.conexion.abrirNavegador();
	}

	@And("^llenar campo usario (.*) y contrasena (.*)$")
	public void diligenciarFormulario(String userName, String pass) {
		this.loginPage = new LoginPage(driver);
		this.loginPage.diligenciarFormulario(userName, pass);
	}
	
	@And("^llegar a la opcion agregar empleado$")
	public void llegarAddEmployee() {
		this.pimPage = new PimPage(driver);
		this.pimPage.llegarAddEmployee();
	}
	
	@When("^al diligenciar first name (.*) y last name (.*)$")
	public void AddEmployee(String firstName, String lastName) {
		this.pimPage = new PimPage(driver);
		this.pimPage.AddEmployee(firstName, lastName);
	}
	
	
	@And("^buscar empleado (.*)$")
	public void searchEmployee(String employee) {
		this.leavePage = new LeavePage(driver);
		this.leavePage.searchEmployee(employee);
	}
	
	@Then("^al encontrar el empleado (.*) y seleccionar una accion (.*)$")
	public void actionsEmployee(String employee, String action) {
		this.leavePage = new LeavePage(driver);
		this.leavePage.actionsEmployee(employee, action);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}