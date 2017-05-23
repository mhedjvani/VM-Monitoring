#******************************************************************************************
# 
# TITLE:       VM Monitoring - Stage 1 
#              
# MODULE:      Templates for Unsupervised Learning
#
# DESCRIPTION: The objective of this process is to explore how different virtual machines (VM) 
# (mostly on Cloud / big-data platforms) may perform running different programmes, under 
# different resource allocations and potentially find an approach to quickly identify faulty
# runs (anomalies) by leveraging unsupervised machine learning techniques
#              
# AUTHOR:      Mohammad Hejvani
#
# Last Modified (& By - if not by Author):
# DD/MM/YYY
#
# INPUTS:      
# 1. Concatenated runs of several programmes on different virtual machines / settings 
# (separate csv files) 
#
# OUTPUTS:
# 1. Time-series charts for each programme * VM combination showing resource utilisation
# 2. Variable importance (i.e. resource -> performance) for each programme * VM combination
# 3. Clusters plots (and those isolated clusters if exist in separate csv files) for each 
# programme * VM combination
#
# Duration: ~30 Sec for each VM
#              
# STEPS:  
# Step 1 Load libs & prep the env, assign parameters, etc.
# Step 2 Sourcing data (& codes if used external)
# Step 3 Process the data / data audit / initial visualisation (discovery)
# Step 4 Perform modelling techniques
# Step 5 Export the results
#
#******************************************************************************************




#### Step 1 Load libs & prep the env, assign parameters, etc. #### 


## Libs - install.packages first if used first time

# General rocessing
require("foreach")
require("doParallel")
require("doSNOW")
require("compiler")

# Data processing
require("sqldf")
require("reshape2")

# Visualisation
require("ggplot2")
require("gridExtra")

# Modelling packages
require("rpart")
require("cluster")
require("fpc")   

# Local parameters
cpu_to_use=4      # Used for parallel processing if needed
VM='T1'     # Indicate the virtual machine of interest

# Directory paths - change to your local drive / server as appropriate
sourcepath="C:\\Users\\hedjv\\Documents\\Code\\R\\Cloud_Monitoring\\"
path=paste("C:\\Users\\hedjv\\Dropbox\\VM-Profile-Classifier\\2015\\T-All\\",sep="")
output=paste("C:\\Users\\hedjv\\Documents\\Output\\VM-Profile-Classifier\\2015\\T-All Output\\",VM,"\\",sep="")




#### Step 2 Sourcing data (& codes if used external - note: place source codes in 'sourcepath') #### 


## Source the raw data 

# Get the list of all csv files in drive / server location
list=list.files(path,pattern=".csv")

# Limit the list to those who follow the naming pattern of ineterest (i.e. VM)
list=list[grep(VM,list)]

# Read in data from the list and label as required
base=foreach(i=1:length(list),.combine=rbind)%do%{
  run=read.csv(paste(path,list[i],sep=""))
  cbind(run,server_code=i,file=list[i])
}

# Check number of programmes (i.e. loop iterations)
paste0("The number of programmes tested on ",VM," are ",length(unique(base$server_code)), sep="")




#### Step 3 Process the data / data audit / initial visualisation (discovery) #### 


## Initial prep

# Format control
base$Timestamp=strptime(base$Timestamp,"%d-%b-%g %I:%M:%S")
base$Run=as.factor(base$Run)

## Parallel loop -- removed unless needed
# cl <- makeCluster(cpu_to_use, type="SOCK")
# registerDoSNOW(cl)

#Record the start time
StartTime = Sys.time()

# Using loop to process all inputs separately as per similar requirements 
# Not: if building the initial draft / testing, use j = 1 (for the first iteration) and comment 
# the loop start / end

# Loop starts here
var.importance=
  foreach(j=1:length(list), .combine=rbind
  )%do%{
    
    # Print the programme name
    print(paste("Running model for ",list[j]))            
    
    # Split the data based on server_code
    split_data=split(base, base$server_code)
    
    # Split by each server_code
    temp=split_data[[j]]
    
    # Cut the data and only retain the numerical average for resource variables
    see=temp[c(1,2,15,grep("average",names(temp)))]
    
    # Sort by TS
    see=see[order(see$Run,see$Timestamp),]
    
    # Mark the start and end of each run for each programme
    s=min(as.numeric(as.character(see$Run)))
    e=max(as.numeric(as.character(see$Run)))
    
    # Scale the CPU Avg
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("cpu.usage.average",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=(look[1]/100) # Benchmark for CPU 100
      cbind(seq,seq_n,Run=i,var)
    }
    
    # Assign names
    names(fun)=c("Seq","Seq_Norm","Run","cpu.usage.average")
    
    # Write into a separate frame
    fun$Run=as.factor(fun$Run)
    cpu=fun
    
    # Create plot object
    p1=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=cpu.usage.average, colour=Run))+
      theme(legend.position="none")
    
    # Scale the Disk Avg
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("disk.usage.average",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=(look[1])/50000 # Benchmark for Disk 50000
      cbind(seq,seq_n,Run=i,var)
    }
    
    # Assign names
    names(fun)=c("Seq","Seq_Norm","Run","disk.usage.average")
    
    # Write into a separate frame
    fun$Run=as.factor(fun$Run)
    disk=fun
    
    # Create plot object
    p2=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=disk.usage.average, colour=Run))+
      theme(legend.position="none")
    
    # Scale the Memory Avg 
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("mem.usage.average",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=(look[1])/100 # Benchmark for Memory 100
      cbind(seq,seq_n,Run=i,var)
    }
    
    # Assign names
    names(fun)=c("Seq","Seq_Norm","Run","mem.usage.average")
    
    # Write into a separate frame
    fun$Run=as.factor(fun$Run)
    mem=fun
    
    # Create plot object
    p3=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=mem.usage.average, colour=Run))+
      theme(legend.position="none")
    
    # Scale the Throughput Avg
    fun=foreach(i=s:e, .combine=rbind)%do%{
      look=see[which(see$Run==i),][grep("ThroughputAverage",names(see))]
      seq=seq(1,nrow(look))
      seq_n=(seq-min(seq))/(max(seq)-min(seq))
      var=look[1]
      cbind(seq,seq_n,Run=i,var)
    }
    
    # Assign names
    names(fun)=c("Seq","Seq_Norm","Run","ThroughputAverage")
    
    # Write into a separate frame
    fun$Run=as.factor(fun$Run)
    TP=fun
    
    # Create plot object
    p4=ggplot(data=fun)+
      geom_line(aes(x=Seq_Norm,y=ThroughputAverage, colour=Run))+
      theme(legend.position="none") 
    
    # Save Combined Plots
    jpeg(file = paste(output,"visual_",list[j],".jpeg",sep=""))
    grid.arrange(p1, p2, p3, p4, top = paste(list[j]), ncol = 1)
    dev.off()
    
    # Combine all the resource variables
    data=Reduce(function(x,y) merge(x,y, by=c("Seq","Seq_Norm","Run"), all=TRUE), list(cpu, disk, mem, TP))
    data2=data[c("cpu.usage.average","disk.usage.average","mem.usage.average","ThroughputAverage")]
    
    # Disregard the variables which are generally below a threshold set by the Expert
    if(quantile(data2$cpu.usage.average,0.75)<0.2){data2$cpu.usage.average<-NULL}
    if(quantile(data2$disk.usage.average,0.75)<0.2){data2$disk.usage.average<-NULL}
    if(quantile(data2$mem.usage.average,0.75)<0.2){data2$mem.usage.average<-NULL}
    
    # Fit a CART model to discover the important predictors of performance
    rpart.fit=rpart(ThroughputAverage~.,data=data2)
    
    # Extract the list of important variables
    var.importance=data.frame(rpart.fit$variable.importance)
    var.importance=data.frame(cbind(paste(list[j]),rownames(var.importance),var.importance))
    colnames(var.importance)=c("Task","Variable","Importance")
    var.importance$Importance=var.importance$Importance/sum(var.importance$Importance) # Scale / normalise 
    
    
    ## Clustering prep
    
    # Aggregate the important predictors for each programme * run into basic statistics 
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
    
    # Combine all
    clust.data=clust.data[order(as.numeric(as.character(clust.data$Run))),]
    clust.data2=clust.data[-grep("Run",names(clust.data))]
    
    
    
    
    #### Step 4 Perform modelling techniques #### 
    
    
    ## Clustering
    
    # Extract the distance matrix - Minkowski: P = 1 for Euclidean and 2 for Manhattan 
    d=dist(clust.data2, method = "minkowski", p = 2)
    
    # Fit a PAM cluster
    pamk.best <- pamk(d)
    pam.fit=pam(d, pamk.best$nc, diss=TRUE)
    pam.clust=cbind(Run=clust.data$Run,clust.data2,Cluster=pam.fit$clustering)
    
    # Export the scored runs
    write.csv(pam.clust,paste(output,"Scores_",list[j],".csv",sep=""))
    
    # Save the cluster plot
    jpeg(file = paste(output,"clusters_",list[j],".jpeg",sep=""))
    clusplot(pam.fit, labels=3, main=paste("Clusters for ",list[j],sep=""))
    dev.off()
    
    ## Find isolated clusters
    clust.info=data.frame(pam.fit$clusinfo)
    isolated=row.names(clust.info[which(clust.info$size<=2),]) # 2 is suggested by the Expert
    
    if(length(isolated)>=1){
      
      isolated.clusters=foreach(i=(1:length(isolated)), .combine=rbind)%do%{
        isolated.sets=pam.clust[which(pam.clust$Cluster==paste(isolated[i])),]
        isolated.sets
      }
    }
    
    # Export the isolated cluster list (if exisit)
    if(exists("isolated.clusters")==TRUE){
      write.csv(isolated.clusters,paste(output,"isolated_",list[j],".csv",sep=""))
      rm(isolated.clusters)
    }
    
    var.importance
    
  }
# Loop ends here 

#Record the end time
EndTime = Sys.time()

## Stop the cluster
# stopCluster(cl)

# Report the Loop process time
paste("It took ", EndTime-StartTime, " to complete the loop process", sep = "")




#### Step 5 Export the [remaining] results #### 

# Export the Variable Importance
write.csv(var.importance,paste(output,VM,".var.imp.csv",sep=""))
