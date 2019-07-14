# Link
http://maven.apache.org/plugins/maven-assembly-plugin/assembly-component.html

# Understand
maven-assembly-plugin 简单理解就是一个打包工具，我们将需要打进包的文件告诉它，它便可以帮助我们打包了。

1. 我们可以一个文件一个文件告诉它：
```
  <files>
    <file>
    </file>
  </files>
```
2. 也可以把一个文件组告诉它：
```
  <fileSets>
    <fileSet>
    </fileSet>
  </fileSets>
```
3. 也可以项目中的依赖文件告诉它：
```
  <dependencySets>
    <dependencySet>
    </dependencySet>
  </dependencySets>
```
