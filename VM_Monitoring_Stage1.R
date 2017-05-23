#*****************************************************************************************
# TITLE:            VM Monitoring - Stage 1 
#              
# MODULE:           Templates for 
#
# DESCRIPTION: 
#
#
#              
# AUTHOR:           Mohammad Hejvani
#
# Last modified By: N/A
#
# Last updated:     16/02/2017
#
# INPUTS:                  
#
# OUTPUTS:     
#              
# STEPS:  1. 
#
#
#
#
#
#***********************************************************************************************


#### Step 1 Prepare data


## Load libs & Env Prep

# Libs loading
require("foreach")
require("doParallel")
require("doSNOW")
require("compiler")
require("sqldf")
require("reshape2")
require("ggplot2")
require("gridExtra")
require("rpart")
require("cluster")
require("fpc")   

# Local parameters
cpu_to_use=4
VM='T1'

# # Directory Paths - Home
# sourcepath="C:\\Users\\hedjv\\Documents\\Code\\R\\Cloud_Monitoring\\"
# path=paste("C:\\Users\\hedjv\\Dropbox\\VM-Profile-Classifier\\2015\\T-All\\",sep="")
# output=paste("C:\\Users\\hedjv\\Documents\\Output\\VM-Profile-Classifier\\2015\\T-All Output\\",VM,"\\",sep="")

# Directory Paths - Work
sourcepath="C:\\Users\\mhejvani\\Documents\\MyProjects\\IAAS_ML\\Codes\\"
path="C:\\Users\\mhejvani\\Documents\\MyProjects\\IAAS_ML\\Data\\T-All\\"
output="C:\\Users\\mhejvani\\Documents\\MyProjects\\IAAS_ML\\Outputs\\VM1\\"


## Sourcing Data (& Codes)


# Get the List of All csv Files
list=list.files(path,pattern=".csv")

# Limit the list to the focused VM
list=list[grep(VM,list)]

# Read in Data
base=foreach(i=1:length(list),.combine=rbind)%do%{
  run=read.csv(paste(path,list[i],sep=""))
  cbind(run,server_code=i,file=list[i])
}

#Check number of programmes
paste0("The number of programmes tested on ",VM," are ",length(unique(base$server_code)), sep="")

# Format Control
base$Timestamp=strptime(base$Timestamp,"%d-%b-%g %I:%M:%S")
base$Run=as.factor(base$Run)


## Parallel Loop -- removed

# cl <- makeCluster(cpu_to_use, type="SOCK")
# registerDoSNOW(cl)

start.time=Sys.time()

var.importance=
  foreach(j=1:length(list), .combine=rbind
  )%do%{
    
    # Print the Programme Name (Task)
    print(paste("Running Model for ",list[j]))            
    
    # List based on server_code
    split_data=split(base, base$server_code)
    
    # Split to Isolate each server_code
    temp=split_data[[j]]
    
    # Cut the Data
    see=temp[c(1,2,15,grep("average",names(temp)))]
    
    # Sort by TS
    see=see[order(see$Run,see$Timestamp),]
    
    # Mark the Start and End of Runs for each Task
    s=min(as.numeric(as.character(see$Run)))
    e=max(as.numeric(as.character(see$Run)))
    
    # Scale the CPU Avg & Create Plot Object
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("cpu.usage.average",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=(look[1]/100) # Benchmark for CPU 100
      cbind(seq,seq_n,Run=i,var)
    }
    
    names(fun)=c("Seq","Seq_Norm","Run","cpu.usage.average")
    
    cpu=fun
    
    p1=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=cpu.usage.average, colour=as.factor(Run)))
    
    # Scale the Disk Avg & Create Plot Object
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("disk.usage.average",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=(look[1])/50000 # Benchmark for Disk 50000
      cbind(seq,seq_n,Run=i,var)
    }
    
    names(fun)=c("Seq","Seq_Norm","Run","disk.usage.average")
    
    disk=fun
    
    p2=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=disk.usage.average, colour=as.factor(Run)))
    
    # Scale the Memory Avg & Create Plot Object
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("mem.usage.average",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=(look[1])/100 # Benchmark for Memory 100
      cbind(seq,seq_n,Run=i,var)
    }
    
    names(fun)=c("Seq","Seq_Norm","Run","mem.usage.average")
    
    mem=fun
    
    p3=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=mem.usage.average, colour=as.factor(Run)))
    
    # Scale the Throughput Avg & Create Plot Object
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("ThroughputAverage",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=look[1]
      cbind(seq,seq_n,Run=i,var)
    }
    
    names(fun)=c("Seq","Seq_Norm","Run","ThroughputAverage")
    
    TP=fun
    
    p4=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=ThroughputAverage, colour=as.factor(Run))) 
    
    # Save Combined Plots
    jpeg(file = paste(output,"visual_",list[j],".jpeg",sep=""))
    grid.arrange(p1, p2, p3, p4, ncol=1)
    dev.off()
    
    # Combine All the Variables
    data=Reduce(function(x,y) merge(x,y, by=c("Seq","Seq_Norm","Run"), all=TRUE), list(cpu, disk, mem, TP))
    
    data2=data[c("cpu.usage.average","disk.usage.average","mem.usage.average","ThroughputAverage")]
    
    # Disregard the Variables which are Generally Below a Set Threshold
    if(quantile(data2$cpu.usage.average,0.75)<0.2){data2$cpu.usage.average<-NULL}
    if(quantile(data2$disk.usage.average,0.75)<0.2){data2$disk.usage.average<-NULL}
    if(quantile(data2$mem.usage.average,0.75)<0.2){data2$mem.usage.average<-NULL}
    
    # Fit a CART Model to Discover the Important Variables
    rpart.fit=rpart(ThroughputAverage~.,data=data2)
    
    # Extract the Important Variables
    var.importance=data.frame(rpart.fit$variable.importance)
    var.importance=data.frame(cbind(paste(list[j]),rownames(var.importance),var.importance))
    colnames(var.importance)=c("Task","Variable","Importance")
    var.importance$Importance=var.importance$Importance/sum(var.importance$Importance)
    
    # Aggregate for each Run
    n=nrow(var.importance)
    
    clust.data=foreach(i=1:n, .combine=cbind)%do%{
      
      Mean=aggregate(data[rownames(var.importance)[i]],by=list(data$Run),FUN=mean)
      colnames(Mean)=c("Run",paste("mean_",rownames(var.importance)[i],sep=""))
      
      Median=aggregate(data[rownames(var.importance)[i]],by=list(data$Run),FUN=median)
      colnames(Median)=c("Run",paste("median_",rownames(var.importance)[i],sep=""))
      
      Min=aggregate(data[rownames(var.importance)[i]],by=list(data$Run),FUN=min)
      colnames(Min)=c("Run",paste("min_",rownames(var.importance)[i],sep=""))
      
      Max=aggregate(data[rownames(var.importance)[i]],by=list(data$Run),FUN=max)
      colnames(Max)=c("Run",paste("max_",rownames(var.importance)[i],sep=""))
      
      Var=aggregate(data[rownames(var.importance)[i]],by=list(data$Run),FUN=var)
      colnames(Var)=c("Run",paste("var_",rownames(var.importance)[i],sep=""))
      
      Agg=Reduce(function(x,y) merge(x,y, by=c("Run"), all=TRUE), list(Mean,Median,Min,Max,Var)) 
      
      Agg
      
    }
    
    clust.data2=clust.data[-grep("Run",names(clust.data))]
    
    # Extract the Distance Matrix
    d=dist(clust.data2, method = "minkowski", p = 2)
    
    # Fit a PAM Cluster
    pamk.best <- pamk(d)
    pam.fit=pam(d, pamk.best$nc, diss=TRUE)
    pam.clust=cbind(Run=clust.data$Run,clust.data2,Cluster=pam.fit$clustering)
    
    # Export the Scored Runs
    write.csv(pam.clust,paste(output,"Scores_",list[j],".csv",sep=""))
    
    # Save the Cluster Plot
    jpeg(file = paste(output,"clusters_",list[j],".jpeg",sep=""))
    clusplot(pam.fit, labels=3, main=paste("Clusters for ",list[j],sep=""))
    dev.off()
    
    # Find isolated clusters
    clust.info=data.frame(pam.fit$clusinfo)
    isolated=row.names(clust.info[which(clust.info$size<=2),])
    
    if(length(isolated)>=1){
      
      isolated.clusters=foreach(i=(1:length(isolated)), .combine=rbind)%do%{
        isolated.sets=pam.clust[which(pam.clust$Cluster==paste(isolated[i])),]
        isolated.sets
      }
    }
    
    if(exists("isolated.clusters")==TRUE){
      write.csv(isolated.clusters,paste(output,"isolated_",list[j],".csv",sep=""))
      rm(isolated.clusters)
    }
    
    var.importance
    
  }
end.time=Sys.time()

paste0("This run took ",end.time-start.time," to complete")

# stopCluster(cl)

# Export the Variable Importance
write.csv(var.importance,paste(output,".var.imp.csv",sep=""))