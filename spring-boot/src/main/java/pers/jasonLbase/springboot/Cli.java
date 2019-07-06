package pers.jasonLbase.springboot;

import java.util.List;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class Cli implements ApplicationRunner{
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		List<String> d = args.getOptionValues("testss");
		System.out.println("============ cli =============");
	}

}
