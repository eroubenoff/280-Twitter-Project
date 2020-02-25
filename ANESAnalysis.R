##############################
#Loading Packages
#############################

library(lda)
library(slam)
#install.packages("~/stmdev/rcode/stm_0.03.07.tar.gz", type="source", repos=NULL)
library(stm)

source("~/stmdev/rcode/stm/R/ploteffect.R")
source("~/stmdev/rcode/stm/R/plot.stm.R")

#############################
#Cleaning Data
#############################

#setwd("../Data")
setwd("~/90days/twitter/")

dataldac <- readLdac("final_anes.csv")
vocabcoarse <- read.csv("final_anes_vocab.csv")$term
meta <- read.csv("final_anes_metadata.csv")

dataldac <- dataldac[meta$pid_summary>0 & meta$highest.grade.completed>0 & meta$age>0]
meta <- meta[meta$pid_summary>0 & meta$highest.grade.completed>0 & meta$age>0,]

data <- prepDocuments(dataldac, vocabcoarse, meta=meta)
documents <- data$documents
vocab <- data$vocab
meta <- data$meta

#####################################
#Model with fear treatment and PID interaction
#####################################

#Select Model
set.seed(01238)
models <- selectModel(documents, vocab, 60, prevalence=~s(meta$pid_summary)+ s(meta$age) + s(meta$highest.grade.completed) + s(meta$highest.grade.completed*meta$pid_summary), runs=50)
plotModels(models)

mod <- models$runout[[10]]
plot.STM(mod, n=3, threshold=3, numtopics=50, labeltype="frex")
mod$settings$seed

#Recover selected model
#seed: 6261705
mod <- stm(documents, vocab, 3, prevalence=~meta$treatment, seed=6261705)
labeltopics(mod)

#Interaction plot Education and war topic
prep21 <- prep.plot(mod, meta$highest.grade.completed, topics=53, theta.uncertainty="Global", span=2, ci.level=.9, documents=documents, subset=as.numeric(meta$pid_summary<3))
ploteffect(prep21, choosetopics=1,model=mod, ylab="Probability of Topic", xlab="Years of Education", labeltype="prob", printlegend=F, ylim=c(0,.1), xlim=c(13,17), linecol="blue", main="STM War Topic and Education")

prep20 <- prep.plot(mod, meta$highest.grade.completed, topics=53, theta.uncertainty="Global", span=2, ci.level=.9, documents=documents, subset=as.numeric(meta$pid_summary>=3))
ploteffect(prep20, choosetopics=1,model=mod, ylab="Mean Topic Proportions", xlab="Years of Education", labeltype="prob", xlim=c(0,1), add=T)

text(16, .09, "Democrat", col="blue", cex=1.2)
text(14.5, .03, "Republican", col="red", cex=1.2)


#ANES Coding comparison
anes <- 3
meta$warall <- ifelse((meta$mippol1_code1==anes | meta$mippol1_code2==anes | meta$mippol1_code3==anes | meta$mippol1_code4==anes | meta$mippol1_code5==anes |
                        meta$mippol1_code6==anes | meta$mippol1_code7==anes | meta$mippol1_code8==anes),1,NA)
meta$warall[is.na(meta$warall)] <- 0
main <- "ANES War Topic and Education"
sub <- meta[meta$pid_summary<3,]
fit <- loess(sub$warall ~ sub$highest.grade.completed, span=2)
newdat <- data.frame(x=seq(min(sub$highest.grade.completed), max(sub$highest.grade.completed)))
pred <- predict(fit, newdata=newdat$x, se=T)
plot(pred$fit ~ newdat$x, type="l", ylim=c(0,.1), col="blue", xlim=c(13,17), xlab="Years of Education", ylab="Probability of Topic", main=main)
lines((pred$fit + 1.64*pred$se)~ newdat$x, col="blue", lty=2)
lines((pred$fit - 1.64*pred$se)~ newdat$x, col="blue", lty=2)

sub <- meta[meta$pid_summary>3,]
fit <- loess(sub$warall ~ sub$highest.grade.completed, span=2)
newdat <- data.frame(x=seq(min(sub$highest.grade.completed), max(sub$highest.grade.completed)))
pred <- predict(fit, newdata=newdat$x, se=T)
lines(pred$fit ~ newdat$x, type="l", ylim=c(min(pred$fit-1.64*pred$se, na.rm=T), max(pred$fit +1.64*pred$se, na.rm=T)), col="red")
lines((pred$fit + 1.64*pred$se)~ newdat$x, col="red", lty=2)
lines((pred$fit - 1.64*pred$se)~ newdat$x, col="red", lty=2)
text(14, .02, "Republican", col="red", cex=1.2)
text(14.5,.08, "Democrat", col="blue", cex=1.2)


#Comparison with hand coding
topmat <- matrix(nrow=nrow(meta), ncol=ncol(mod$theta))
for(i in 1:nrow(meta)){
#  meta$mip1top[i] <- which(mod$theta[i,]==max(mod$theta[i,]))
  topmat[i,] <- mod$theta[i,]>.2
}
sumtopics <- apply(topmat,2,sum)
toptopics <- matrix(nrow=4, ncol=3)
toptopics[1,] <- c(sumtopics[56], "The Economy", sum(meta$mippol1_code1==50 |meta$mippol1_code1==51 | meta$mippol1_code1==52 | meta$mippol1_code1==53 | meta$mippol1_code1==54 | meta$mippol1_code1==55))
toptopics[2,] <- c(sumtopics[53] + sumtopics[29], "War, or Iraq War", sum(meta$mippol1_code1==4 | meta$mippol1_code1==3))
toptopics[3,] <- c(sumtopics[47] + sumtopics[30], "Don't Know",sum(meta$mippol1_code1==95))
#toptopics[4,] <- c(sumtopics[14] + sumtopics[55], "Budget",sum(meta$mippol1 <- code1==38))
toptopics[4,] <- c(sumtopics[40] + sumtopics[19] + sumtopics[20] + sumtopics[36], "Employment",sum(meta$mippol1_code1==27))
#toptopics[5,] <- c(sumtopics[58], "Terrorism", sum(meta$mippol1_code1==5))

rownames(toptopics) <- c("Economy", "War or Iraq War", "Don't Know", "Unemployment and Job")
colnames(toptopics) <- c("STM", "", "Hand-Coding")

library(xtable)
xtable(toptopics)


#Individual responses hand coding
meta[,29:36][is.na(meta[,29:36])] <- 0
ok <- (mod$theta[,58]>.2)  & meta$mippol1_code1!=5 & meta$mippol1_code2!=5 & meta$mippol1_code3!=5 & meta$mippol1_code4!=5 & meta$mippol1_code5!=5 & meta$mippol1_code6!=5 & meta$mippol1_code7!=5 & meta$mippol1_code8!=5 #&
#  meta$mippol1_code2!=3 & meta$mippol1_code3!=3 & meta$mippol1_code4!=3 & meta$mippol1_code5!=3 & meta$mippol1_code6!=3 & meta$mippol1_code7!=3 & meta$mippol1_code8!=3 & meta$mippol1_code1!=3

meta$mip_1[ok]
meta$mippol1_code1[ok]

ok <- ((mod$theta[,58]<.2) & (meta$mippol1_code1==5 | meta$mippol1_code2==5 | meta$mippol1_code3==5 | meta$mippol1_code4==5 | meta$mippol1_code5==5 | meta$mippol1_code6==5 | meta$mippol1_code7==5 | meta$mippol1_code8==5))
                                                    #meta$mippol1_code1==3 | meta$mippol1_code2==3 | meta$mippol1_code3==3 | meta$mippol1_code4==3 | meta$mippol1_code5==3 | meta$mippol1_code6==3 | meta$mippol1_code7==3 | meta$mippol1_code8==3))
meta$mip_1[ok]
meta$mippol1_code1[ok]

#Number of categories for each response
topmat <- matrix(nrow=nrow(meta), ncol=ncol(mod$theta))
for(i in 1:nrow(meta)){
#  meta$mip1top[i] <- which(mod$theta[i,]==max(mod$theta[i,]))
  topmat[i,] <- mod$theta[i,]>.2
}
catnum <- apply(topmat,1,sum)
anescatnum <- apply(meta[,29:36],1,function (x) sum(x!=0))
table(anescatnum, catnum)
table(anescatnum, catnum)/apply(table(anescatnum,catnum),1,sum)
t(table(anescatnum, catnum))/(apply(table(anescatnum,catnum),2,sum))
