package executors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

class FolderProcessor extends RecursiveTask<List<String>>
{
	private static final long serialVersionUID = 1L;
	private String fileName;
	private String extension;
	
	

	public FolderProcessor(String fileName, String extension) {
		super();
		this.fileName = fileName;
		this.extension = extension;
	}



	@Override
	protected List<String> compute() {
		List<String> results = new ArrayList<String>();
		//FolderProcessor tasks to store the subtasks that are going to process the subfolders stored in the folder
	    List<FolderProcessor> tasks = new ArrayList<FolderProcessor>();
		File f = new File(fileName);
	    File[] contents = f.listFiles();
		if(contents!=null)
		{
			for(File curr: contents)
			{
				if(curr.isDirectory())
				{
					FolderProcessor subTask = new FolderProcessor(curr.getAbsolutePath(), extension);
					subTask.fork(); //fork out the subtask, async call
					tasks.add(subTask);
				}
				else
				{
					if(checkExtension(curr.getName()))
						results.add(curr.getAbsolutePath());
					
				}
			}
			for(FolderProcessor subTask: tasks)
				results.addAll(subTask.join()); // synchronus call
		}
		return results;
	}
	public boolean checkExtension(String fileName)
	{
		return fileName.endsWith(extension);
	}
}

public class FolderProcessorUsingFJP 
{
	public static void main(String[] args) 
	{
		ForkJoinPool pool = new ForkJoinPool(20);
		FolderProcessor mainTask = new FolderProcessor("/home/rahul/git/MyJavaProjects", "java");
		pool.execute(mainTask);
		do{
			System.out.printf("Parallelism=%d \nActive Threads=%d \nTask count=%d \nSteal count=%d"
					,pool.getParallelism(),pool.getActiveThreadCount(),pool.getQueuedTaskCount(),pool.getStealCount());
		}
		while(!mainTask.isDone());
		pool.shutdown();
		List<String> results = mainTask.compute();
		for(String s:results)
			System.out.println(s);
	}
}