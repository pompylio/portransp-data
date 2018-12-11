# Bibliotecas -------------------------------------------------------------
require(shiny); require(miniUI); require(portransp); require(googledrive);
require(tidyverse); require(lubridate); require(reshape2); require(DT)

# Base de dados -----------------------------------------------------------
db_structure <- readRDS("base/db_structure.rds")

# Funções -----------------------------------------------------------------
interval <- function(data, by, inter){
  db <- max(as.Date(data))
  if (missing(inter)) inter <- NULL else db <- floor_date(db, "month")
  db <- seq(from = db, to = Sys.Date(), by = by)
  if (any(inter == "last")){
    ld <- function(x) ceiling_date(x, "month") - days(1)
    db <- sapply(db, FUN = ld, simplify = FALSE)
    db <- do.call("c", db)
  }
  if (any(duplicated(db))){
    db <- unique(db)
  }
  return(db)
  }

# Aplicação ---------------------------------------------------------------
refresh <- function() {
  # UI ----
  ui <- miniPage(
    miniTitleBar("Dados do Portal da Transparência"),
    miniContentPanel(
      column(
        width = 12,
        selectInput(
          inputId = "inp_item",
          label = "Conjunto de dados",
          choices = c("", paste(db_structure$id, db_structure$item, db_structure$subitem)),
          selected = "",
          width = "100%"),
        uiOutput("inp_file"),
        uiOutput("ref_log"),
        uiOutput("reg_log"))),
    miniButtonBlock(
      uiOutput("create"),
      uiOutput("save"),
      uiOutput("refresh"),
      uiOutput("drive_update"),
      uiOutput("drive_upload"),
      uiOutput("downgrade")
      )
    )
  # SERVER ----
  server <- function(input, output, session) {

    observeEvent(input$inp_item, {
      if(input$inp_item == ""){
        output$inp_file <- renderUI({""})
      } else {
        choices <-
        paste(
          db_structure[db_structure$id == str_extract(string = input$inp_item, "\\d{1,2}"), ]$id_file,
          db_structure[db_structure$id == str_extract(string = input$inp_item, "\\d{1,2}"), ]$file_des
        )
      output$inp_file <- renderUI({
        selectInput(
          inputId = "inp_file",
          label = "Arquivo",
          choices = choices,
          selected = choices[1],
          width = "100%"
          )
      })
      }
    })

    infodata <- eventReactive(c(input$inp_item, input$inp_file), {
      opendata <- str_extract(string = input$inp_item, pattern = "\\d{1,2}")
      itemfile <- str_extract(string = input$inp_file, pattern = "\\d{1,2}")
      structure <- db_structure[db_structure$id_file == itemfile, ]
      directory <- list.dirs(path = paste0("data/", structure$dataset))
      origfile <- paste0(directory, "/", structure$nm_file_des, ".rds")
      logsfile <- paste0(directory, "/log_", structure$nm_file_des, ".rds")
      if (file.exists(logsfile)){
        datalogs <- readRDS(file = logsfile[grepl(pattern = "log_", x = logsfile)])
        datalogs <- tail(datalogs, 6)
      } else {
        datalogs <- NULL
      }
      if(is.null(datalogs)){
        interlog <- interval("2014-01-01", by = structure$by)
      } else {
        if (structure$by %in% c("month", "year")){
          interlog <- interval(tail(datalogs$date, n = 1), by = structure$by, inter = "last")
        } else {
          interlog <- interval(tail(datalogs$date, n = 1), by = structure$by)
        }
        if (length(interlog) < 2){
          interlog <- NULL
        } else {
          interlog <- interlog[2:length(interlog)]
        }
      }
      infodata <- list(
        opendata = opendata,
        itemfile = itemfile,
        structure = structure,
        directory = directory,
        origfile = origfile,
        logsfile = logsfile,
        datalogs = datalogs,
        interlog = interlog)
      infodata
    })

    observeEvent(input$inp_file, {
      output$printlog <- renderPrint({infodata()})
      output$ref_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
      if(file.exists(infodata()$logsfile)){
        output$create <- renderUI({""})
        output$refresh <- renderUI({actionButton(inputId = "refresh",label = "Atualizar")})
        output$reg_log <- renderUI({tags$br()})
        output$drive_upload <- renderUI({
          actionButton(
            inputId = "drive_upload",
            label = "Upload Drive")})
        output$drive_update <- renderUI({
          actionButton(
            inputId = "drive_update",
            label = "Update Drive")})
        output$downgrade <- renderUI({
          actionButton(
            inputId = "downgrade",
            label = "Retornar"
          )
        })
      } else {
        output$create <- renderUI({actionButton(inputId = "create",label = "Criar")})
        output$refresh <- renderUI({""})
        output$reg_log <- renderUI({h4("Conjunto de dados não encontrado!")})
        output$drive_update <- renderUI({""})
        output$drive_upload <- renderUI({""})
      }
      })

# Até aqui sem necessidade de alteração -----------------------------------

    observeEvent(input$create, {
      logsfile <- data.frame(date = Sys.Date(), zip = "", file = "", nrows = 0L, ncol = 0L, log = "", update = Sys.Date(), stringsAsFactors = FALSE)
      n <- length(infodata()$interlog)
      withProgress(message = "Criando ...", value = 0, {
        for(i in 1:n){
          interval <- gsub(pattern = "-", replacement = "", x = as.character(infodata()$interlog[i]))
          reference <- substr(interval, 1, nchar(infodata()$structure$formatdate))
          namefile <- paste0(unique(infodata()$structure$dataset), "_", reference, ".zip")
          downfile <- list.files(path = infodata()$directory, pattern = namefile, full.names = TRUE)
          incProgress(1/n, detail = paste(namefile, interval))
          Sys.sleep(0.1)
          if (file.size(downfile) == 0 || length(downfile) == 0){
            try(potr_download(opendata = infodata()$opendata, reference = reference, destfile = infodata()$directory), silent = TRUE)
            downfile <- list.files(path = infodata()$directory, pattern = namefile, full.names = TRUE)
            if(file.size(downfile) == 0 || length(downfile) == 0){
              output$printlog <- renderPrint({paste(i, ymd(interval), infodata()$structure$dataset, "(URL não encontrada)")})
              output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
              next()
            }
            }
          struct <- potr_structure_zip(zipfile = downfile)$list_files; print(struct); print(infodata())
          struct <- struct[which(struct$Name == struct[grepl(pattern = infodata()$structure$files, x = struct$Name), ]$Name),]
          if (struct$Nrow > 0){
            file_unzip <- unzip(zipfile = downfile, files = struct$Name, exdir = infodata()$directory); print(file_unzip)
            origfile <- read.csv2(file = file_unzip, colClasses = "character", check.names = FALSE, comment.char = "", nrows = struct$Nrow)
            colnames(origfile) <- iconv(colnames(origfile), from = "Latin1", to = 'ASCII//TRANSLIT')
            colnames(origfile) <- gsub(pattern = "[^[:alnum:] ]", replacement = " ", x = colnames(origfile))
            colnames(origfile) <- str_squish(colnames(origfile))
            colnames(origfile) <- str_trim(colnames(origfile))
            colnames(origfile) <- gsub(" ", "_", colnames(origfile))
            colnames(origfile) <- str_to_upper(colnames(origfile))
            if (infodata()$structure$nm_file_des %in% c("despesas_liquidacao_empenhosimpactados", "despesas_pagamento_empenhosimpactados")){
              origfile <- origfile %>% filter(substr(CODIGO_EMPENHO, 7, 11) == "26428")
              } else if (infodata()$structure$nm_file_des %in% c("despesas_empenho", "despesas_liquidacao", "despesas_pagamento")){
                origfile <- origfile %>% filter(CODIGO_ORGAO == "26428")
                } else if (infodata()$structure$nm_file_des %in% c("cadastro")){
                  origfile <- origfile %>% filter(COD_ORG_LOTACAO == "26428" | COD_ORG_EXERCICIO == "26428")
                  } else if (infodata()$structure$nm_file_des == "despesas"){
                    origfile <- origfile %>% filter(CODIGO_GESTAO == "26428" | CODIGO_UNIDADE_ORCAMENTARIA == "26428")
                    }
            if (nrow(origfile) > 0){
              origfile$DATA_REFERENCIA <- ymd(interval)
              origfile[, sapply(origfile, class) == "character"] <- apply(X = origfile[, sapply(origfile, class) == "character"], MARGIN = 2, FUN = function(y){iconv(x = y, from = "Latin1", to = "UTF-8")})
              output$printlog <- renderPrint({paste(struct$Name, "(concluído)")})
              output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
              logsfile <- data.frame(date = ymd(interval), zip = namefile, file = struct$Name, nrows = nrow(origfile), ncols = ncol(origfile), log = "Concluído", update = Sys.Date(), stringsAsFactors = FALSE)
              break()
              } else {
                output$printlog <- renderPrint({paste(struct$Name, "(Planilha sem dados do IFB)")})
                output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
                log <- data.frame(date = ymd(interval), zip = namefile, file = struct$Name, nrows = 0L, ncols = 0L, log = "Planilha sem dados do IFB", update = Sys.Date(), stringsAsFactors = FALSE)
                logsfile <- bind_rows(logsfile, log)
                next()
                }
            unlink(x = file_unzip)
            } else{
              output$printlog <- renderPrint({paste(struct$Name, "(Planilha sem dados)")})
              output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
              logsfile <- data.frame(date = ymd(interval), zip = namefile, file = struct$Name, nrows = 0L, ncols = 0L, log = "Planilha sem dados do IFB", update = Sys.Date(), stringsAsFactors = FALSE)
              next()
            }
          }
        if(nrow(logsfile) > 1){
          logsfile <- logsfile[2:nrow(logsfile), ]
          }
        })
      output$reg_log <- renderUI({""})
      output$create <- renderUI({""})
      output$save <- renderUI({
        actionButton(
          inputId = "save",
          label = "Salvar")
      })
      observeEvent(input$save, {
        saveRDS(object = origfile, file = infodata()$origfile)
        saveRDS(object = logsfile, file = infodata()$logsfile)
        n <- 2
        withProgress(message = "Salvando", value = 0, {
          for(i in 1:2){
            incProgress(1/n, detail = paste("Parte", i))
            Sys.sleep(0.1)
            }
          })
        output$printlog <- renderPrint({paste(infodata()$origfile, "e", infodata()$logsfile, "salvos com sucesso!!")})
        output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
        })
      observeEvent(input$save, {
        output$refresh <- renderUI({
          actionButton(
            inputId = "refresh",
            label = "Atualizar")
          })
        output$drive_upload <- renderUI({
          actionButton(
            inputId = "drive_upload",
            label = "Upload Drive"
            )
          })
        })
    })

    observeEvent(input$refresh, {
      origfile <- readRDS(infodata()$origfile)
      logsfile <- readRDS(infodata()$logsfile)
      infodata <- infodata()
      interval <- gsub(pattern = "-", replacement = "", x = as.character(infodata$interlog))
      withProgress(message = "Atualizando ...", value = 0, {
        for(i in 1:length(interval)){
          reference <- substr(interval[i], 1, nchar(infodata$structure$formatdate))
          namefile <- paste0(unique(infodata$structure$dataset), "_", reference, ".zip")
          downfile <- list.files(path = infodata$directory, pattern = namefile, full.names = TRUE)
          tryunzip <- try(unzip(zipfile = downfile, list = TRUE), silent = TRUE)
          if(!file.exists(downfile) || file.size(downfile) == 0 || length(downfile) == 0 || class(tryunzip) == "try-error"){
            try(potr_download(opendata = infodata$opendata, reference = reference, destfile = infodata$directory), silent = TRUE)
            downfile <- list.files(path = infodata$directory, pattern = namefile, full.names = TRUE)
            if(file.size(downfile) == 0 || length(downfile) == 0){
              print(paste(i, ymd(interval[i]), infodata$structure$dataset, "(URL não encontrada)"))
              next()
              }
            }
          struct <- potr_structure_zip(zipfile = downfile)$list_files
          struct <- struct[which(struct$Name == struct[grepl(pattern = infodata$structure$files, x = struct$Name), ]$Name),]
          if (struct$Nrow > 0){
            file_unzip <- paste0(infodata$directory, "/", struct$Name)
            if(!file.exists(file_unzip)){
              file_unzip <- try(unzip(zipfile = downfile, files = struct$Name, exdir = infodata$directory), silent = TRUE)
            }
            origfile2 <- read.csv2(file = file_unzip, colClasses = "character", check.names = FALSE, comment.char = "", nrows = struct$Nrow)
            colnames(origfile2) <- iconv(colnames(origfile2), from = "Latin1", to = 'ASCII//TRANSLIT')
            colnames(origfile2) <- gsub(pattern = "[^[:alnum:] ]", replacement = " ", x = colnames(origfile2))
            colnames(origfile2) <- str_squish(colnames(origfile2))
            colnames(origfile2) <- str_trim(colnames(origfile2))
            colnames(origfile2) <- gsub(" ", "_", colnames(origfile2))
            colnames(origfile2) <- str_to_upper(colnames(origfile2))
            if (infodata$structure$nm_file_des %in% c("despesas_liquidacao_empenhosimpactados", "despesas_pagamento_empenhosimpactados")){
              origfile2 <- origfile2 %>% filter(substr(COD_EMPENHO, 7, 11) == "26428")
              } else if (infodata$structure$nm_file_des %in% c("despesas_empenho", "despesas_liquidacao", "despesas_pagamento")){
                origfile2 <- origfile2 %>% filter(CODIGO_ORGAO == "26428")
                } else if (infodata$structure$nm_file_des %in% c("cadastro")){
                  origfile2 <- origfile2 %>% filter(COD_ORG_LOTACAO == "26428" | COD_ORG_EXERCICIO == "26428")
                } else if (infodata$structure$nm_file_des == "despesas"){
                    origfile2 <- origfile2 %>% filter(CODIGO_GESTAO == "26428" | CODIGO_UNIDADE_ORCAMENTARIA == "26428")
                  }
            if (nrow(origfile2) > 0){
              origfile2$DATA_REFERENCIA <- ymd(interval[i])
              origfile2[, sapply(origfile2, class) == "character"] <- apply(X = origfile2[, sapply(origfile2, class) == "character"], MARGIN = 2, FUN = function(y){iconv(x = y, from = "Latin1", to = "UTF-8")})
              msg <- paste(reference, "(concluído)"); print(msg)
              logsfile2 <- data.frame(date = ymd(interval[i]), zip = namefile, file = struct$Name, nrows = nrow(origfile2), ncols = ncol(origfile2), log = "Concluído", update = Sys.Date(), stringsAsFactors = FALSE)
              logsfile <- bind_rows(logsfile, logsfile2)
              origfile <- bind_rows(origfile, origfile2)
            } else {
              msg <- paste(reference, "(Planilha sem dados do IFB)"); print(msg)
              logsfile2 <- data.frame(date = ymd(interval[i]), zip = namefile, file = struct$Name, nrows = nrow(origfile2), ncols = ncol(origfile2), log = "Planilha sem dados do IFB", update = Sys.Date(), stringsAsFactors = FALSE)
              logsfile <- bind_rows(logsfile, logsfile2)
            }
          unlink(x = file_unzip)
          } else{
            msg <- paste(reference, "(Planilha sem dados)"); print(msg)
            logsfile2 <- data.frame(date = ymd(interval[i]), zip = namefile, file = struct$Name, nrows = 0, ncols = 0, log = "Planilha sem dados", update = Sys.Date(), stringsAsFactors = FALSE)
            logsfile <- bind_rows(logsfile, logsfile2)
          }
          incProgress(1/length(interval), detail = msg)
          Sys.sleep(0.1)
          }
        })
      output$create <- renderUI({""})
      output$upload <- renderUI({""})
      output$save <- renderUI({
        actionButton(
          inputId = "save",
          label = "Salvar")
      })
      observeEvent(input$save, {
        saveRDS(object = origfile, file = infodata()$origfile)
        saveRDS(object = logsfile, file = infodata()$logsfile)
        n <- 2
        withProgress(message = "Salvando", value = 0, {
          for(i in 1:2){
            incProgress(1/n, detail = paste("Parte", i))
            Sys.sleep(0.1)}})
        output$printlog <- renderPrint({paste(infodata()$origfile, "e", infodata()$logsfile, "salvos com sucesso!!")})
        output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
        output$refresh <- renderUI({
          actionButton(
            inputId = "refresh",
            label = "Atualizar")})
        output$drive_update <- renderUI({
          actionButton(
            inputId = "drive_update",
            label = "Upload Drive")})
        })
      })

    observeEvent(input$downgrade, {
      origfile <- readRDS(infodata()$origfile)
      logsfile <- readRDS(infodata()$logsfile)
      lastdate <- nrow(origfile)-tail(logsfile$nrows,1)
      origfile <- origfile[1:lastdate, ]
      unlink(paste0(infodata()$directory, "/", logsfile$zip[nrow(logsfile)]))
      logsfile <- logsfile[1:(nrow(logsfile)-1), ]
      saveRDS(object = origfile, file = infodata()$origfile)
      saveRDS(object = logsfile, file = infodata()$logsfile)
    })

    observeEvent(input$drive_upload, {
        httr::set_config(httr::use_proxy ("10.198.0.15", 8080))
        require(googledrive)
        drive_auth(oauth_token = "security/googledrive_token.rds")
        drive_upload(infodata()$origfile, as_id("1dwBHZIt21Lvb8kq07Efm9FUpeWE2zlWs"), paste0(infodata()$structure$nm_file_des,".rds"))
        drive_upload(infodata()$logsfile, as_id("1dwBHZIt21Lvb8kq07Efm9FUpeWE2zlWs"), paste0("log_",infodata()$structure$nm_file_des,".rds"))
        drive_deauth()
        output$printlog <- renderPrint({"salvos com sucesso na pasta 'R/projects/portransp-data/data' do googledrive!!"})
        output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
        })
    observeEvent(input$drive_update, {
        httr::set_config(httr::use_proxy ("10.198.0.15", 8080))
        require(googledrive)
        drive_auth(oauth_token = "security/googledrive_token.rds")
        files_drive <- drive_ls(as_id("1dwBHZIt21Lvb8kq07Efm9FUpeWE2zlWs"))
        nameorig <- paste0(infodata()$structure$nm_file_des,".rds"); print(nameorig)
        namelogs <- paste0("log_", nameorig); print(namelogs)
        drive_update(file = as_id(files_drive[files_drive$name == nameorig, ]$id), media = infodata()$origfile)
        drive_update(file = as_id(files_drive[files_drive$name == namelogs, ]$id), media = infodata()$logsfile)
        drive_deauth()
        output$printlog <- renderPrint({"salvos com sucesso na pasta 'R/projects/portransp-data/data' do googledrive!!"})
        output$reg_log <- renderUI({verbatimTextOutput("printlog", placeholder = TRUE)})
        })
    }
  runGadget(ui, server, viewer = dialogViewer(dialogName = "Portransp", width = 800, height = 800))
  }
refresh()