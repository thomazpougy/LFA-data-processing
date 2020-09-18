
import functions

def cabecalho():
    print(" ")
    print("#########################################################")
    print("#          Data Processing ToolKit for LFA-USP          #")
    print("#                                                       #")
    print("#      CPC    |    SMPS    |   Termo49   |    AE33      #")
    print("#                                                       #")
    print("#   Thomaz Assaf Pougy    •     thomazpougy@usp.br      #")
    print("#########################################################")
    print("#                                                       #")
    print("#      USE CRTL+C TO EXIT THE PROGRAM AT ANY TIME       #")
    print("#                                                       #")
    print("#########################################################")

def menu_1():
    while True:
        print("###########################################")
        print(" ")
        print("CHOOSE THE INSTRUMENT")
        print("1 - CPC")
        print("2 - SMPS")
        print("3 - Termo49i")
        print("4 - AE33")
        print(" ")

        try:
            choosen_instrument = int(
                input("Type the number corresponding to your choice: "))
            if not 1 <= choosen_instrument <= 4:
                raise ValueError()
        except ValueError:
            print(" ")
            print("###########################################")
            print("Error! Please type a number in the interval")
            print("###########################################")
            print(" ")
        else:
            if (choosen_instrument == 1):
                str_choosen_instrument = "CPC"
            elif (choosen_instrument == 2):
                str_choosen_instrument = "SMPS"
            elif (choosen_instrument == 3):
                str_choosen_instrument = "Termo49i"
            else:
                str_choosen_instrument = "AE33"
            print(" ")
            break

    return str_choosen_instrument, choosen_instrument

def menu_2(str_choosen_instrument):
    all_metadata = functions.r_metadata()
    print("###########################################")
    print(" ")
    print("Check the metadata for " + str_choosen_instrument)
    print(" ")
    print("Metadata:")
    print(all_metadata[[str_choosen_instrument]])
    print(" ")

    while True:
        try:
            change_cell = input("Type 'Y' to continue or type the number corresponding to the metadata you want to change: ")
            if not ((change_cell == 'Y') or (change_cell == 'y') or (0 <= int(change_cell) <= 6)) : raise ValueError()
            elif not ((change_cell == 'Y') or (change_cell == 'y')) : change_cell = int(change_cell)
        except ValueError:
            print(" ")
            print("###########################################")
            print("Error! Please type a number in the interval")
            print("###########################################")
            print(" ")
        else :
            print(" ")
            break

    return change_cell

def menu_change(str_choosen_instrument, change_cell):
    all_metadata = functions.r_metadata()
    print("Change the metadata " + all_metadata.index[change_cell] + " to instrument " + str_choosen_instrument)
    print(" ")

    new_value = input("Type the new value for the metadata:")
    print(" ")

    all_metadata[str_choosen_instrument][change_cell]=new_value
    functions.w_metadata(all_metadata)

def menu_3():
    while True:
        print("")
        print("###########################################")
        print(" ")
        print("CHOOSE AN OPTION")
        print("1 - Choose another instrumet to process")
        print("2 - End executions/Exit")
        print(" ")

        try:
            choice_3 = int(input("Digite o numero correspondente a sua escolha: "))
            if not 1 <= choice_3 <= 2:
                raise ValueError()
        except ValueError:
            print(" ")
            print("###########################################")
            print("Error! Please type a number in the interval")
            print("###########################################")
            print(" ")
        else:
            print(" ")
            break
    return choice_3

def msg_execution_finished():
    print(" ")
    print("####################################################################################")
    print("-------------------------------  EXECUTION FINISHED --------------------------------")
    print("####################################################################################")
    print("The .csv file with the processed data can be found the the same folder as the script")
    print("####################################################################################")
    print(" ")

def msg_choose_raw_data_folder(str_choosen_instrument):
    print("###########################################")
    print(" ")
    print("Choose the folder with the raw data for " + str_choosen_instrument)
    print(" ")
    print("###########################################")
