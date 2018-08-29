package huwn.study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClient {
    /*
    文件长传
     */
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //获取文件系统
        Configuration configuration = new Configuration();
        //参数优先级排序：
        // （1）客户端代码中设置的值 >（2）classpath下的用户自定义配置文件 >（3）然后是服务器的默认配置
        configuration.set("dfs.replication","2");
       FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

       //上传本地文件到hdfs上
        fs.copyFromLocalFile(new Path("E:\\tmp\\flume.log"),new Path("/user/fromWindow"));

        //关闭资源
        fs.close();
    }

    /*
    文件下载到本地
     */
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException {
        //获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"),configuration);
        fs.copyToLocalFile(new Path("/user/fromWindow"),new Path("E:\\tmp\\downloadFromHdfs.txt"));
        fs.close();
    }
    /*
    文件夹删除
     */
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

        // 2 执行删除
        fs.delete(new Path("/user/fromWindow"), true);

        // 3 关闭资源
        fs.close();
    }

    /*
    文件改名
     */
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

        // 2 修改文件名称
        fs.rename(new Path("/hello.txt"), new Path("/hello6.txt"));

        // 3 关闭资源
        fs.close();
    }

    /*
    文件详情查看
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

        //方法1
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/Test/CombinFile"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("文件名:" + fileStatus.getPath().getName());
            System.out.println("文件大小:" +fileStatus.getBlockSize());
        }

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/user/Test/CombinFile"), true);
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("文件名：" + fileStatus.getPath().getName());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                //主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }

        fs.close();
    }

    /*
    通过IO流的方式上传文件
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hello4.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hello1.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hello1.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://CentOS01:9000"), configuration);

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


}
