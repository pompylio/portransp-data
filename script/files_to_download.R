require(portransp)
require(lubridate)
require(tidyverse)
load(file = "base/sysdata.rda") ; rm(potrms, potrurl)
db_structure <- data.frame(id = "", group = "", item = "", subitem = "", dataset = "", update = "", formatdate = "", by = "", stringsAsFactors = FALSE)
for(i in 1:nrow(potrdt)){
  destdir <- paste0("data/",potrdt$dataset[i])
  fl_dwl <- list.files(path = destdir, pattern = potrdt$dataset[i], full.names = TRUE)
  fl_dwl <- fl_dwl[grepl(pattern = ".zip", x = fl_dwl)]
  if (length(fl_dwl)==0){
    potr_download_last(opendata = potrdt$id[i], destfile = destdir)
    fl_dwl <- list.files(path = destdir, pattern = potrdt$dataset[i], full.names = TRUE)
    print("Iniciando download")
  } else {
    if (file.size(max(fl_dwl)) == 0){
      print("o arquivo zip selecionado não possui arquivos")
      next()
    } else {
      print("Vários arquivos, selecionado o último")
      fl_dwl <- max(fl_dwl)
    }
  }
  print(paste(fl_dwl, "Iniciado"))
  struct <- potr_structure_zip(fl_dwl)$list_files$Name
  struct <- gsub(pattern = "[0-9]+", replacement = "",x = struct)
  print(paste(struct, "Finalizado"))
  struct <- data.frame(files = struct, stringsAsFactors = FALSE)
  struct <- cbind(potrdt[potrdt$id == i, ], struct)
  db_structure <- bind_rows(db_structure, struct)
}
db_structure <- db_structure[2:nrow(db_structure), ]
db_structure$files <- str_replace_all(string = db_structure$files, c("‡"="c", "Æ" = "a"))
db_structure$file_des <- gsub(pattern = "[\\.]|[\\_]|csv|zip|\\(|\\)", replacement = " ", db_structure$files)
db_structure$file_des <- str_trim(string = db_structure$file_des)
db_structure$nm_file_des <- gsub(pattern = " ", replacement = "_", str_to_lower(db_structure$file_des))
db_structure$id_file <- as.character(seq(from = 1, to = nrow(db_structure)))
saveRDS(db_structure, "base/db_structure.rds")
