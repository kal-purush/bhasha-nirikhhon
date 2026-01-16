//Liam Robb, lr003
import java.util.Collections;
import java.util.Vector;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import javax.swing.table.*;

public class Dispatcher extends javax.swing.JFrame {

    private static int pidCounter;
    // ** add a 'where we are int' to simulate LL
    private int llp = 0;
    //
    private ArrayList<Process> ready;
    private ArrayList<Process> blocked;
    private Process running;

    public Dispatcher() {
        initComponents();
        pidCounter = 10; // ** fine for last, but maybe change to demonstrate mem slots?
        ready = new ArrayList<>();
        blocked = new ArrayList<>();
        // ** swap out this next for initializing it to 1000 mem: pid at 10 is fine, mem = prio
        pidCounter--; // ** make our unused task 9
        //running = new Process(pidCounter, 1337, ProcessStatus.BLOCKED); // outside of init, hardcoded
        pidCounter++; // start ready/memory list with mem
        ready.add(0, new Process(pidCounter, 1000, ProcessStatus.FREE)); // status "FREE"
        ++pidCounter;
        //--** testing
        //initialSet(); // ** maybe could still have an implementation of this
        //--
        drawTable();
    }


    @SuppressWarnings("unchecked")

    private void initComponents() { // seem 'global' in relation to program

        mySeperator = new javax.swing.JSeparator();
        addproc = new javax.swing.JButton();
        procLabel = new javax.swing.JLabel();
        scroller = new javax.swing.JScrollPane();
        firstTable = new javax.swing.JTable();
        controlArea = new javax.swing.JLabel();
        //processBlock = new javax.swing.JButton();
        //processUnblock = new javax.swing.JButton();
        processKill = new javax.swing.JButton();
        //endAll = new javax.swing.JButton();
        inputPriority = new javax.swing.JTextField();
        //reset = new javax.swing.JButton();
        exit = new javax.swing.JButton();
        labelBig = new javax.swing.JLabel();
        timeExceeded = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        addproc.setText("Add");
        addproc.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                addProcessButtonPress(evt);
            }
        });

        procLabel.setText("Add Process");

        firstTable.setModel(new javax.swing.table.DefaultTableModel(
                new Object [][] { // 2d array

                },
                new String [] {  // heading and define length of first array
                        "Process ID", "Memory", "Status"
                }
        ) {
            boolean[] canEdit = new boolean [] {
                    false, false, false
            };

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        scroller.setViewportView(firstTable);
        if (firstTable.getColumnModel().getColumnCount() > 0) {
            firstTable.getColumnModel().getColumn(0).setResizable(false);
            firstTable.getColumnModel().getColumn(1).setResizable(false);
            firstTable.getColumnModel().getColumn(2).setResizable(false);
        }

        controlArea.setText("Control Panel");

        //processBlock.setText("Block Selected");
        //processBlock.addActionListener(new java.awt.event.ActionListener() {
        //    public void actionPerformed(java.awt.event.ActionEvent evt) {
        //        blockProcessButtonPress(evt);
        //    }
        //});

        timeExceeded.setText("Collect Garbage");
        timeExceeded.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                timeSliceExceededButtonPress(evt);
            }
        });

        /*
        processUnblock.setText("Unblock Selected");
        processUnblock.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                unblockProcessButtonPress(evt);
            }
        }); */

        processKill.setText("Kill Selected");
        processKill.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                killProcessButtonPress(evt);
            }
        });
        /*
        endAll.setText("Kill All");
        endAll.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                killsAllButtonPress(evt);
            }
        });*/

        inputPriority.setText("Memory Request as Integer"); // ** took out directions for easier testing

        /*
        reset.setText("Reset");
        reset.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                resetButtonPress(evt);
            }
        });*/

        exit.setText("Exit");
        exit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                exitButtonPress(evt);
            }
        });

        labelBig.setText("First Fist Memory Management System, with Garbage Collection");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
                layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(layout.createSequentialGroup()
                                .addContainerGap()
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addComponent(scroller)
                                        .addComponent(mySeperator)
                                        .addGroup(layout.createSequentialGroup()
                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                        .addGroup(layout.createSequentialGroup()
                                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                                        .addGroup(layout.createSequentialGroup()
                                                                                .addGap(20, 20, 20)
                                                                                .addComponent(inputPriority, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                                                .addComponent(addproc))
                                                                        .addComponent(labelBig))
                                                                .addGap(170, 170, 170)
                                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)//processBlock next line
                                                                        //.addComponent(processBlock, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                                        ))//.addComponent(processUnblock, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                                                        .addGroup(layout.createSequentialGroup()
                                                                .addGap(48, 48, 48)
                                                                .addComponent(procLabel)))
                                                .addGap(18, 18, 18)
                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                        .addComponent(controlArea)
                                                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                                                                .addGroup(layout.createSequentialGroup()
                                                                        .addComponent(processKill, javax.swing.GroupLayout.PREFERRED_SIZE, 129, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                        .addGap(18, 18, 18)
                                                                        )//.addComponent(reset, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                                                .addGroup(layout.createSequentialGroup()
                                                                        //.addComponent(endAll, javax.swing.GroupLayout.PREFERRED_SIZE, 129, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                        .addComponent(timeExceeded, javax.swing.GroupLayout.PREFERRED_SIZE, 129, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                        .addGap(18, 18, 18)
                                                                        .addComponent(exit, javax.swing.GroupLayout.PREFERRED_SIZE, 129, javax.swing.GroupLayout.PREFERRED_SIZE))))))
                                            //

                                .addContainerGap())
        );
        layout.setVerticalGroup(
                layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                .addContainerGap()
                                .addComponent(scroller, javax.swing.GroupLayout.PREFERRED_SIZE, 477, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addComponent(mySeperator, javax.swing.GroupLayout.PREFERRED_SIZE, 10, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addGroup(layout.createSequentialGroup()
                                                .addComponent(controlArea)
                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        //.addComponent(processBlock)
                                                        .addComponent(processKill)
                                                        ))//.addComponent(reset)))
                                        .addGroup(layout.createSequentialGroup()
                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                .addComponent(procLabel)
                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        .addComponent(addproc)
                                                        .addComponent(inputPriority, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addGroup(layout.createSequentialGroup()
                                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        //.addComponent(processUnblock)
                                                        .addComponent(timeExceeded)
                                                        //.addComponent(endAll)
                                                        .addComponent(exit))
                                                .addGap(11, 11, 11))
                                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                                .addComponent(labelBig)
                                                .addContainerGap())))
        );

        pack();
    }
    // Kills all processes.
    // ** <<<<<<
    private void killsAllButtonPress(java.awt.event.ActionEvent evt) { // maybe keep as a start over button
        ready.clear(); // ** could be repurposed as garb collector -- gutted --
        blocked.clear();
        running = null;
        DefaultTableModel model = (DefaultTableModel) firstTable.getModel();
        model.setRowCount(0);
        ready = new ArrayList<>();
        blocked = new ArrayList<>();
    }

    // Kills the selected Process (That is, the process currently highlighted on the table).
    // ** can be repurposed to deallocate memory, can we change pid to some null value
    private void killProcessButtonPress(java.awt.event.ActionEvent evt) { //seems mostly ok
        DefaultTableModel model = (DefaultTableModel) this.firstTable.getModel(); //** needed
        int row = firstTable.getSelectedRow();

        Vector rowData = (Vector)model.getDataVector().elementAt(row); // used to be final
        int targetPid = (int)rowData.elementAt(0); // ** pid can be memory or still process ID
        // ** (int)rowData.elementAt(0) will give the status bit which we are looking for
        ProcessStatus status = (ProcessStatus)rowData.elementAt(2);
        for (Process p : ready) { // fun other way to iterate through
            if(p.getPid() == targetPid){
                p.setStatus(ProcessStatus.FREE);
                p.setPid(0);

                //ready.remove(p);
                break;
            }
        }
        reDrawTable();

        //model.removeRow(row); // ** this is where we might make diff

        // ** could not remove it, and instead just change status

        //Remove the process from ArrayList
        /* //byebye for now
        switch(status){ // **  probabaly can get rid of any row altering logic, or use this as part of garbo
            case FREE:
                running = null;
                running = contextSwitch();
                reDrawTable();
                break;

            case TAKEN: // ** useful
                for (Process p : ready) {
                    if(p.getPid() == targetPid){
                        ready.remove(p);
                        break;
                    }
                }
                break;

            case BLOCKED:
                for (Process p : blocked) {
                    if(p.getPid() == targetPid){
                        blocked.remove(p);
                        break;
                    }
                }
                break;

            default:
                System.err.println("Invalid status");
                break;
        }     */
    }

    // Adds a Process, given a priority, to the ready ArrayList.
    // ** this is changed to ask for memory
    // ** >>>>> add place
    private void addProcessButtonPress(java.awt.event.ActionEvent evt) {

        if(isInteger(inputPriority.getText(),10)){ // ** 10 size of buffer
            int getPriority = Integer.parseInt(inputPriority.getText());
            //**get the resources needed
            DefaultTableModel model = (DefaultTableModel) firstTable.getModel();
            firstTable.setModel(model); // ** may not need

            if(getPriority > 0){
                if(running == null) // ** new one shouldn't ever be null, but leaving this for now
                    running = new Process(pidCounter,getPriority,ProcessStatus.FREE); //** never here now
                // ** default everything to ready when we add it, that has 1 to 1 with 'taken mem'

                else {
                  // ** check if there is space by ff (loop)
                  if(isThereSpace(ready, getPriority)){
                    addMem(getPriority);
                    ++pidCounter;
                  }
                  else {
                    dothegarb();
                    if(isThereSpace(ready, getPriority)){
                      addMem(getPriority);
                      ++pidCounter;
                    }
                    else{
                      JOptionPane.showMessageDialog(null, "There isn't a big enough memory slot, even after garbage collection.");
                    }
                    // ** message
                    // ** really we need to garbo compact and check again
                    //JOptionPane.showMessageDialog(null, "There isn't a big enough memory slot.");

                  }
                }


                reDrawTable();
            }
            else JOptionPane.showMessageDialog(null, "Please enter a positive integer value greater than 0.");
        } // ** these effectively deal with bad input, as they exlude non integer and negatives

        else JOptionPane.showMessageDialog(null, "Please enter an integer value.");
    }

    // Resets the program to its initial state.
    // ** probably need to take this out or modify it, reset needs to wipe memory if modifying
    private void resetButtonPress(java.awt.event.ActionEvent evt) {
        // ** this is state of table when initial set is called
        ready.clear();
        blocked.clear();
        running = null;
        DefaultTableModel model = (DefaultTableModel) firstTable.getModel();
        model.setRowCount(0);
        pidCounter = 10;
        ready = new ArrayList<>();
        blocked = new ArrayList<>();
        running = new Process(pidCounter, 1000, ProcessStatus.FREE); // this is oitside of init
        ++pidCounter;
        initialSet(); // ** should comment this out too
        drawTable();
    }

    // Exits the program.
    // ** can keep, but is useless

    private void exitButtonPress(java.awt.event.ActionEvent evt) {
        System.exit(0);
    }


    // time exceed context switch
    // ** think this is useless, this juggles the two highest priority processses
    // ** which i don't think has a one to one parallel with ff memory mangagement
    // ** <<<<<<
    private void timeSliceExceededButtonPress(java.awt.event.ActionEvent evt) {
        int skip = 0;
        for (int i = 0; i < ready.size(); i++){ // this increments everything in list rn
          if (ready.get(llp).getStatus() == ProcessStatus.FREE){
            skip = garbageCollectInThisSpot();
            i += skip;
          }
          modInc(); // ** took out else, in off chance that was wrong descision
        }
        //running = contextSwitch2(); // ** just call the garbage man on it
        reDrawTable();
    }

    //smaller garbofunc

    private void dothegarb(){
      int skip = 0;
      for (int i = 0; i < ready.size(); i++){ // this increments everything in list rn
        if (ready.get(llp).getStatus() == ProcessStatus.FREE){
          skip = garbageCollectInThisSpot();
          i += skip;
        }
        modInc(); // ** took out else, in off chance that was wrong descision
      }
      //running = contextSwitch2(); // ** just call the garbage man on it
      reDrawTable();
    }



    private int garbageCollectInThisSpot(){
      // ** big if statement incoming
      if (ready.size() <= 1) return 0; //add message?
      if (llp == 0){
        // ** at the beginning, check here and next in line
        if (ready.get(llp).getStatus() == ProcessStatus.FREE && ready.get(llp + 1).getStatus() == ProcessStatus.FREE){
          //System.out.println("what does it add to, first 2");
          //combine
          ready.get(llp).setMem(ready.get(llp).getMem() + ready.get(llp + 1).getMem()); //get process and mutate it

          ready.remove(llp+1); // with +1 and +0 result same
          return 1;
        }
      }
      else if (llp == ready.size() - 1){ // this one works properly
        //System.out.println("2nd if");
        // ** at the end, check here and the one before
        if (ready.get(llp).getStatus() == ProcessStatus.FREE && ready.get(llp - 1).getStatus() == ProcessStatus.FREE){
          //combine
          //System.out.println("2nd if shouldnt get here at first");
          ready.get(llp).setMem(ready.get(llp).getMem() + ready.get(llp - 1).getMem()); //get process and mutate it
          //get rid of other
          ready.remove(llp + 1);// ready.remove pics as if 1 indecied
          //llp--;
          return 1;
        }
      }
      else {
        //System.out.println("last else");
        //** check all three, it can still be two sets of two so more ifs to come
        if (ready.get(llp).getStatus() == ProcessStatus.FREE &&
          ready.get(llp - 1).getStatus() == ProcessStatus.FREE &&
          ready.get(llp + 1).getStatus() == ProcessStatus.FREE){
            // all 3 ready to be merged
          ready.get(llp).setMem(ready.get(llp).getMem() + ready.get(llp - 1).getMem() + ready.get(llp + 1).getMem());
          //System.out.println("removed 3");
          // destory the + and - 1's
          ready.remove(llp+1);
          ready.remove(llp-1); //

          llp--;
          return 2;
        }
        else if(ready.get(llp).getStatus() == ProcessStatus.FREE && ready.get(llp + 1).getStatus() == ProcessStatus.FREE){
          ready.get(llp).setMem(ready.get(llp).getMem() + ready.get(llp + 1).getMem()); // destroy next
          ready.remove(ready.get(llp + 2)); // added one to all ready remove
          return 1;
        }
        else if (ready.get(llp).getStatus() == ProcessStatus.FREE && ready.get(llp - 1).getStatus() == ProcessStatus.FREE){
          ready.get(llp).setMem(ready.get(llp).getMem() + ready.get(llp - 1).getMem()); // destroy prev
          ready.remove(llp-1); // aaahhhhhhhhhh soooo many
          llp--;
          return 1;
        }
        else return 0;
      }

      return 0;
    }
    // Blocks the selected process.
    // ** for this there will be no difference between block and kill

    private void blockProcessButtonPress(java.awt.event.ActionEvent evt) {
        DefaultTableModel model = (DefaultTableModel) this.firstTable.getModel();
        int row = firstTable.getSelectedRow();
        Vector rowData = (Vector)model.getDataVector().elementAt(row);

        //If blocking the running process, simply context switch
        if((ProcessStatus)rowData.elementAt(2) == ProcessStatus.FREE){
            running = contextSwitch();
        }

        //Otherwise move the selected process to the blocked list
        else{
            int targetPid = (int)rowData.elementAt(0);
            for (Process p : ready) {
                if(p.getPid() == targetPid){
                    p.setStatus(ProcessStatus.BLOCKED);
                    blocked.add(p);
                    ready.remove(p);
                    break;
                }
            }
        }
        reDrawTable();
    }

    //Resumes the currently selected blocked process.
    // ** I don't think there is any point to "Un" Deallocating memory
    // ** so I'll throw this out or comment out

    private void unblockProcessButtonPress(java.awt.event.ActionEvent evt) {
        DefaultTableModel model = (DefaultTableModel) this.firstTable.getModel();
        int row = firstTable.getSelectedRow();
        Vector rowData = (Vector)model.getDataVector().elementAt(row);

        if((ProcessStatus)rowData.elementAt(2) == ProcessStatus.BLOCKED){
            int targetPid = (int)rowData.elementAt(0);
            for (Process p : blocked) {
                if(p.getPid() == targetPid){
                    p.setStatus(ProcessStatus.TAKEN);
                    ready.add(p);
                    blocked.remove(p);
                    break;
                }
            }
        }
        if(running == null)
            running = contextSwitch();
        reDrawTable();
    }

    public static void main(String args[]) {

        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }


        /* Create and display the form */
        java.awt.EventQueue.invokeLater(() -> {
            new Dispatcher().setVisible(true);
        });
    }
    //** wearehere changed to llp (linklistpointer) for length purposes
    // ** added to check if there is any space in the 'ready list hijacked to hold the memory'
    public boolean isThereSpace(ArrayList<Process> processes, int spaceRequested){

      for (int i = 0; i < processes.size(); i++){ // this increments everything in list rn
        if (processes.get(llp).getStatus() == ProcessStatus.FREE){
          //System.out.println("the status was free");
          //System.out.println(String.valueOf(spaceRequested));
          //System.out.println(String.valueOf(ready.get(llp).getMem()));
          if (ready.get(llp).getMem() >= spaceRequested){
            //System.out.println("did i get here and somehow not add mem?");
            return true;
          }
        }
        modInc(); // ** took out else, in off chance that was wrong descision
      } // ** false alarm, this works, incrementing all including one just added
      return false; //** for now
    }

    // ** need an easier way to think about modular llp incrementing
    public void modInc(){
      if (llp == ready.size() - 1){
        llp = 0; // i think maybe -1 for ready.size
      }
      else {
        llp++; // llp has to have been modified at this point
      }
    }

    //** antoher timesaver, only call after calling isThereSpace
    public void addMem(int m){

      int holder = ready.get(llp).getMem(); // ** old mem slot number, subtract from this
      int newM = holder - m;
      if (newM == 0){
        ready.get(llp).setStatus(ProcessStatus.TAKEN);
        ready.get(llp).setPid(pidCounter);
      }
      else{
        ready.get(llp).setMem(newM);
        ready.add(llp, new Process(pidCounter,m,ProcessStatus.TAKEN));
      }

      //pidCounter++;
      modInc();
    }

    // Populate table with a set of processes
    // ** I think get rid of this, start from nothing in the memory
    // ** this is setupr
    // ** >>>>>>>

    public void populateTable(ArrayList<Process> processes){
        DefaultTableModel model = (DefaultTableModel) firstTable.getModel();
        firstTable.setModel(model);
        processes.stream().map((process) -> {
            Object[] row = new Object[3];
            row[0] = process.getPid();
            //If reading off of blocked list, ignore priority
            if(process.getStatus() == ProcessStatus.BLOCKED)
                row[1] = -1;
            else row[1] = process.getPriority();
            row[2] = process.getStatus();
            return row;
        }).forEachOrdered((row) -> {
            model.addRow(row); // ** adding rows could use this method when expanding or shrinking
        });
    }

    // Populates table with one process

    public void populateTable(Process process){
        /* DefaultTableModel model = (DefaultTableModel) firstTable.getModel();
        firstTable.setModel(model);
        Object[] row = new Object[3];
        row[0] = process.getPid();
        row[1] = "Running";
        row[2] = process.getStatus();
        model.addRow(row);*/
    }

    // Draw the table from lists.

    public void drawTable(){
        if(running != null){
            populateTable(running);
            //** begin gutting

            //Collections.sort(ready);

            //**end gutting so far
            populateTable(ready);
            populateTable(blocked);
        }
        else{
            // ** gutgutgut

            //Collections.sort(ready);

            // ** can't be sorting a link list
            populateTable(ready);
            populateTable(blocked);
        }
    }

    // Erases contents of the table and redraws it.

    public void reDrawTable(){
        DefaultTableModel model = (DefaultTableModel) firstTable.getModel();
        model.setRowCount(0);
        drawTable();
    }

    // Fills the sets with arbitrary initial values.

    public void initialSet(){
        ready.add(new Process(pidCounter, 20, ProcessStatus.TAKEN));
        ++pidCounter;
        ready.add(new Process(pidCounter, 43, ProcessStatus.TAKEN));
        ++pidCounter;
        ready.add(new Process(pidCounter, 18, ProcessStatus.TAKEN));
        ++pidCounter;
        ready.add(new Process(pidCounter, 34, ProcessStatus.TAKEN));
        ++pidCounter;
    }

    // Switches the running process to the process on the Ready list with the highest priority unless the ready list is empty, in which case it returns null.

    public Process contextSwitch(){
        if (ready.size() > 0){
            Process newRunning = new Process(ready.get(0));
            newRunning.setStatus(ProcessStatus.FREE);
            if(running != null){
                running.setStatus(ProcessStatus.BLOCKED);
                blocked.add(running);
            }
            ready.remove(0);
            return newRunning;
        }
        else{
            if(running != null){
                running.setStatus(ProcessStatus.BLOCKED);
                blocked.add(running);
            }
            return null;
        }
    }

    public Process contextSwitch2(){ //for just context switching
        if (ready.size() > 0){
            Process newRunning = new Process(ready.get(0));
            newRunning.setStatus(ProcessStatus.FREE);
            ready.remove(0);
            if(running != null){
                running.setStatus(ProcessStatus.TAKEN);
                ready.add(running);
            }
            return newRunning;
        }
        else{
            if(running != null){
                running.setStatus(ProcessStatus.BLOCKED);
                blocked.add(running);
            }
            return null;
        }
    }
    // input sanitizing

    public static boolean isInteger(String s, int radix) {
        if(s.isEmpty()) return false;
        for(int i = 0; i < s.length(); i++) {
            if(i == 0 && s.charAt(i) == '-') {
                if(s.length() == 1) return false;
                else continue;
            }
            if(Character.digit(s.charAt(i),radix) < 0) return false;
        }
        return true;
    }


    // declaring my gui variables
    //private javax.swing.JButton processBlock;
    private javax.swing.JLabel controlArea;
    private javax.swing.JButton exit;
    //private javax.swing.JButton endAll;
    private javax.swing.JButton processKill;
    private javax.swing.JLabel labelBig;
    private javax.swing.JTextField inputPriority;
    private javax.swing.JButton addproc;
    private javax.swing.JLabel procLabel;
    //private javax.swing.JButton reset;
    private javax.swing.JScrollPane scroller;
    private javax.swing.JSeparator mySeperator;
    private javax.swing.JTable firstTable;
    //private javax.swing.JButton processUnblock;
    private javax.swing.JButton timeExceeded;

}