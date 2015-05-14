#RealTimeMLAnalytics
#install.packages ("rgl")
library (rgl)

setwd("~/01-workspace/scala-akka-spark/scala-spark/gftRealTimeMLAnalytics")
#cluster + 38-dimensional data set
#data frame
clusters_data <- read.csv("~/01-workspace/scala-akka-spark/scala-spark/gftRealTimeMLAnalytics/anomalies/part-00000", header=FALSE)
#random projection
clusters <- clusters_data[1]
data <- data.matrix(clusters_data[-c(1)])
data
rm(clusters_data)

myncol <- ncol(data)
myncol
myrandom <- rnorm (3 * myncol) # 3 * 38 numeros aleatorios con distribucion normal mean=0 sd=1
myrandom
random_projection <- matrix (data=myrandom)
random_projection
random_projection <- matrix (data=myrandom, ncol = 3)
random_projection #matrix of 38 rows and 3 col with random numbers between 0 & 1
myrowsums <-rowSums (random_projection*random_projection)
myrowsums
mysqrtrowsums <- sqrt (myrowsums)
mysqrtrowsums
random_projection_norm <- random_projection / mysqrtrowsums
random_projection_norm

projected_data <- data.frame(data %*% random_projection_norm)# %*% matrix multiplication colnum data = rownum rando_projection_nomr = 38 so we can multiply
projected_data

#data1 <- projected_data[1]
#data1

num_clusters <- nrow (clusters)
num_clusters
num_clusters <- nrow (unique (clusters))
num_clusters
palette <- rainbow (num_clusters)
palette
colors = sapply (clusters, function(c) palette[c])
colors
plot3d (projected_data, col = colors, size=10)

#the dominant feature of the visualization is its “L” shape. The
#points seem to vary along two distinct dimensions, and little in other dimensions.
#This makes sense, because the data set has two features that are on a much larger scale
#than the others. Whereas most features have values between 0 and 1, the bytes-sent
#and bytes-received features vary from 0 to tens of thousands. The Euclidean distance
#between points is therefore almost completely determined by these two features. It’s
#almost as if the other features don’t exist! So, it’s important to normalize away these
#differences in scale to put features on near-equal footing.
  