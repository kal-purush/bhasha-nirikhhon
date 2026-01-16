package com.milleniuminfinity.app.milleniuminfinity.activity;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;

import com.milleniuminfinity.app.milleniuminfinity.activity.employee.AddEmployee;
import com.milleniuminfinity.app.milleniuminfinity.R;
import com.milleniuminfinity.app.milleniuminfinity.activity.employee.EmployeeMenu;
import com.milleniuminfinity.app.milleniuminfinity.activity.employee.ViewEmployee;
import com.milleniuminfinity.app.milleniuminfinity.activity.internet.InternetMenu;
import com.milleniuminfinity.app.milleniuminfinity.domain.internet.Internet;
import com.milleniuminfinity.app.milleniuminfinity.repository.tables.CreateDatabase;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
    }

    /*public void addEmployee(View v)
    {
        Intent intent = new Intent(this, AddEmployee.class);
        startActivity(intent);
    }*/

    public void employeeMenu(View v) throws Exception {
        Intent intent = new Intent(this, EmployeeMenu.class);
        startActivity(intent);
    }

    public void internetMenu(View v)
    {
        Intent intent = new Intent(this, InternetMenu.class);
        startActivity(intent);
    }

    public void viewAll(View v)
    {
        Intent intent = new Intent(this, ViewEmployee.class);
        startActivity(intent);
    }

}