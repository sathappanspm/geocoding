package resource_wrapper;

public class Main {

	public static void main(String[] args) {
		Resource r = new Resource("..",
				"testname", ".", "True", 
			"awskey", "awssecret", "bucket", "econf");
		
		System.out.println(r.pyResource.get_localfpath());

	}

}
