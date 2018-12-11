files <- list.files("data/despesas", pattern = "despesas-", full.names = TRUE)
files2 <- gsub(pattern = "[-]", replacement = "_", x = files)
for(i in 1:length(files)){
  file.rename(from = files[i], to = files2[i])
}
