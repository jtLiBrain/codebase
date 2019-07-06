package pers.jasonLbase.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;

public class IOUtil {
	/**
	 * FileInputStream/FileOutputStream的每次读/写操作都会实际地向硬盘发出数据的读/写，
	 * 以字节为频率，即，每一个字节都要从硬盘读一次，然后再向硬盘写一次
	 * 该方法性能较低
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	@Deprecated
	public void copyPerByte(String fromFile, String toFile) throws IOException {
		FileInputStream in = null;
		FileOutputStream out = null;

		try {
			in  = new FileInputStream(fromFile);
			out = new FileOutputStream(toFile);
			//the int variable c holds a character value in its last 8 bits, and each int value has 32 bits
			int c;
			while ((c = in.read()) != -1) {
				out.write(c);
			}

		} finally {
			close(in);
			close(out);
		}
	}
	
	/**
	 * 该方法由于每2K数据才发生一次硬盘的读/写操作，因此相对copyPerByte()性能有所改进
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyBytes(String fromFile, String toFile) throws IOException {
		FileInputStream in = null;
		FileOutputStream out = null;
		
		try {
			in  = new FileInputStream(fromFile);
			out = new FileOutputStream(toFile);

			byte[] bs = new byte[ 2 * 1024 ];
			while (in.read(bs) != -1) {
				out.write(bs);
			}

		} finally {
			close(in);
			close(out);
		}
	}
	
	/**
	 * BufferedInputStream/BufferedOutputStream默认内部拥有8K的缓存，
	 * 
	 * 当调用BufferedInputStream上的read()方法时，只有当缓存中数据为空时，才发生一次真正的硬盘读操作，
	 * 将BufferedInputStream内部缓存读满；否则只是对缓存进行读操作；
	 * 
	 * 同样对于BufferedOutputStream上的write()方法，只有当缓存完全充满时，才发生一次真正的硬盘写操作，
	 * 将BufferedOutputStream内部缓存清空；否则只是对缓存进行写操作；
	 * 
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyBufferedPerByte(String fromFile, String toFile) throws IOException {
		BufferedInputStream in = null;
		BufferedOutputStream out = null;

		try {
			in  = new BufferedInputStream(new FileInputStream(fromFile));
			out = new BufferedOutputStream(new FileOutputStream(toFile));

			//the int variable c holds a byte value in its last 8 bits
			//and each int value has 32 bits
			int c;
			while ((c = in.read()) != -1) {
				out.write(c);
			}

		} finally {
			close(in);
			close(out);
		}
	}
	
	/**
	 * 相对于copyBufferedPerByte()，由于每次数据吞吐量变大，所以性能有所提升
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyBufferedBytes(String fromFile, String toFile) throws IOException {
		BufferedInputStream in = null;
		BufferedOutputStream out = null;
		
		try {
			in  = new BufferedInputStream(new FileInputStream(fromFile));
			out = new BufferedOutputStream(new FileOutputStream(toFile));
			
			byte[] bs = new byte[ 2 * 1024 ];
			while (in.read(bs) != -1) {
				out.write(bs);
			}
			
		} finally {
			close(in);
			close(out);
		}
	}
	
	/**
	 * 类似copyPerByte()性能较低
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	@Deprecated
	public void copyPerCharacter(String fromFile, String toFile) throws IOException {
		FileReader reader = null;
		FileWriter writer = null;
		
		try {
			reader = new FileReader(fromFile);
			writer = new FileWriter(toFile);
			//the int variable c holds a character value in its last 16 bits, and each int value has 32 bits
			int c;
			while ((c = reader.read()) != -1) {
				writer.write(c);
			}
			
		} finally {
			close(reader);
			close(writer);
		}
	}
	
	/**
	 * 每2K数据才发生一次硬盘的读/写操作，相比copyPerCharacter()性能有所提升
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyCharacters(String fromFile, String toFile) throws IOException {
		FileReader reader = null;
		FileWriter writer = null;
		
		try {
			reader = new FileReader(fromFile);
			writer = new FileWriter(toFile);

			char[] cs = new char[ 2 * 1024 ];
			while (reader.read(cs) != -1) {
				writer.write(cs);
			}
			
		} finally {
			close(reader);
			close(writer);
		}
	}
	
	/**
	 * BufferedReader/BufferedWriter默认内部拥有16K的缓存，
	 * 
	 * 当调用BufferedReader上的read()方法时，只有当缓存中数据为空时，才发生一次真正的硬盘读操作，
	 * 将BufferedReader内部缓存读满；否则只是对缓存进行读操作；
	 * 
	 * 同样对于BufferedWriter上的write()方法，只有当缓存完全充满时，才发生一次真正的硬盘写操作，
	 * 将BufferedWriter内部缓存清空；否则只是对缓存进行写操作；
	 * 
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyBufferedCharacter(String fromFile, String toFile) throws IOException {
		BufferedReader reader = null;
		BufferedWriter writer = null;
		
		try {
			reader = new BufferedReader(new FileReader(fromFile));
			writer = new BufferedWriter(new FileWriter(toFile));
			
			//the int variable c holds a character value in its last 16 bits
			//and each int value has 32 bits
			int c;
			while ((c = reader.read()) != -1) {
				writer.write(c);
			}
			
		} finally {
			close(reader);
			close(writer);
		}
	}
	
	/**
	 * 相对于copyBufferedCharacter()，由于每次数据吞吐量变大，所以性能有所提升
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyBufferedCharacters(String fromFile, String toFile) throws IOException {
		BufferedReader reader = null;
		BufferedWriter writer = null;
		
		try {
			reader = new BufferedReader(new FileReader(fromFile));
			writer = new BufferedWriter(new FileWriter(toFile));
			
			char[] cs = new char[ 2 * 1024 ];
			while (reader.read(cs) != -1) {
				writer.write(cs);
			}
			
		} finally {
			close(reader);
			close(writer);
		}
	}
	
	/**
	 * PrintWriter背后也是采用BufferedWriter实现的，因此它也是带有内存数据缓冲的
	 * 
	 * @param fromFile
	 * @param toFile
	 * @throws IOException
	 */
	public void copyBufferedCharacterLine(String fromFile, String toFile) throws IOException {
		BufferedReader reader = null;
		PrintWriter writer = null;
		
		try {
			reader = new BufferedReader(new FileReader(fromFile));
			writer = new PrintWriter(toFile);
			
			String l;
			while ((l = reader.readLine()) != null) {
				writer.println(l);
			}
			
		} finally {
			close(reader);
			close(writer);
		}
	}
	
	
	
	/**
	 * 
	 * @param in
	 */
	public static void close(InputStream in) {
		if(in != null) {
			try {
				in.close();
				in = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param out
	 */
	public static void close(OutputStream out) {
		if(out != null) {
			try {
				out.close();
				out = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param reader
	 */
	public static void close(Reader reader) {
		if(reader != null) {
			try {
				reader.close();
				reader = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param writer
	 */
	public static void close(Writer writer) {
		if(writer != null) {
			try {
				writer.close();
				writer = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
