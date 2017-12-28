#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

//Ajuta la eliberarea memoriei in care se retin copii unui nod.
#define freeNeighbours(s,i) {\
	(i) = (s);\
	while((i)){\
		(s) = (i);\
		(i) = (i)->next;\
		free((s));\
	}\
}

struct simpleLinkedList {
	struct simpleLinkedList *next;
	int info;
};

int main(int argc, char* argv[]){
	if( argc != 4 ){
		fprintf(stderr,"Not enough command line arguments have been specified!\n");
		return 1;
	}
  	FILE *topFD = fopen( argv[1] , "rt" );
  	if( !topFD ){
  		fprintf(stderr,"I could not open the topology file !\n");
  		return 1;
  	}
  	FILE *imageListFD = fopen( argv[2] , "rt" );
  	if( !imageListFD ){
  		fprintf(stderr,"I could not open the image list file !\n");
  		return 1;
  	}

    //Sunt filtrele sub forma de vectori ce vor fii folosit mai apoi la mofificare
    //imaginilor.
  	signed char smoothMatrix[10] = {1,1,1,1,1,1,1,1,1,9};
  	signed char blurMatrix[10] = {1,2,1,2,4,2,1,2,1,16};
  	signed char sharpenMatrix[10] = {0,-2,0,-2,11,-2,0,-2,0,3};
  	signed char meanRemovalMatrix[10] = {-1,-1,-1,-1,9,-1,-1,-1,-1,1};


    //Se stabilesc rank-ul procesului si numarul de noduri.
    int r, n;
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &r);
	MPI_Comm_size(MPI_COMM_WORLD, &n);

    //Se aloca memorie pentru buffer-ul in care se va citi linie
    //cu linie fisierul de topologie.
	char *readBuffer = (char *)malloc(200 * sizeof(char));
	if( !readBuffer ) {
		fclose(topFD);
 		fclose(imageListFD);
    	MPI_Finalize();
    	fprintf(stderr,"I could not allocate memory !\n");
		return 1;
	}
	size_t len = 200;
	char numberBuffer[7];
	unsigned char a;

    //Se citeste linie cu linie fisierul de topologie.
	while( getline(&readBuffer,&len,topFD) != -1 ){
		for( a = 0 ; a < 7 ; a++ )
			numberBuffer[a] = '\0';
		a = 0;
		while(readBuffer[a] != ':'){
			numberBuffer[a] = readBuffer[a];
			a++;
		}
		if( atoi(numberBuffer) == r ){
			break;
		}
	}
	struct simpleLinkedList *neighbours = NULL, *iterator, *newCell;
	const size_t dim = strlen( readBuffer );
	//S-a gasit linia corespunzatoare rank-ului procesului curent si
	//se incepe citirea vecinilor.
	while(a<dim){
		while( a < dim && !isdigit(readBuffer[a]) )
			a++;

		unsigned char b;

		for( b = 0 ; b < 7 ; b++ )
			numberBuffer[b] = '\0';

		b = 0;

		while( a < dim && isdigit(readBuffer[a]) ){
			numberBuffer[b] = readBuffer[a];
			a++;
			b++;
		}

		newCell = (struct simpleLinkedList*)malloc(sizeof(struct simpleLinkedList));
		if( !newCell ){
			freeNeighbours(neighbours,iterator);
			free(readBuffer);
			fclose(topFD);
	 		fclose(imageListFD);
	    	MPI_Finalize();
	    	fprintf(stderr,"I could not allocate memory !\n");
			return 1;
		}
		newCell->next = NULL;
		newCell->info = atoi(numberBuffer);

		if( neighbours == NULL ){
			neighbours = newCell;
			iterator = newCell;
		} else {
			iterator->next = newCell;
			iterator = newCell;
		}

		while( a < dim && !isdigit(readBuffer[a]) )
			a++;
	}
	MPI_Status recvStatus;
	int i;

	//0 - sonda
	//1 - ecou
	if( r == 0 ){

        //Se trimit sondele vecinilor nodului radacina.
		iterator = neighbours;
		i = 0;
		while(iterator){
			MPI_Send(&i,1,MPI_INT, iterator->info,0,MPI_COMM_WORLD);
			iterator = iterator->next;
		}

		newCell = NULL;
		iterator = neighbours;
		while(iterator){
			MPI_Recv(&i,1,MPI_INT,iterator->info,0,MPI_COMM_WORLD,&recvStatus);
			//Daca nodul-vecin raspunde cu 0 atunci este eliminat din lista de
			//adiacenta.
			if( i == 0 ){
				if( newCell == NULL ){
					newCell = iterator;
					iterator = iterator->next;
					neighbours = iterator;
					free(newCell);
					newCell = NULL;
				} else {
					newCell->next = iterator->next;
					struct simpleLinkedList *aux = iterator;
					iterator = iterator->next;
					free(aux);
				}
			} else {
				newCell = iterator;
				iterator = iterator->next;
			}
		}

        //Se determina numarul de copii.
        iterator = neighbours;
		int numberOfChildren = 0;
		while(iterator){
			numberOfChildren++;
			iterator = iterator->next;
		}

		fscanf(imageListFD,"%d",&i);
		int j;
		//Se itereaza prin imaginilie de prelucrat.
		for( j = 0 ; j < i ; j++ ){
			char pictureName[20], modifiedPictureName[20];
			fscanf(imageListFD,"\n%s %s %s", readBuffer, pictureName, modifiedPictureName);
			FILE *inputImageFD = fopen( pictureName , "rt" );
			if( !inputImageFD ) {
				freeNeighbours(neighbours,iterator);
				free(readBuffer);
	 			fclose(topFD);
			 	fclose(imageListFD);
			    MPI_Finalize();
			    fprintf(stderr,"I could not open the input image file !\n");
				return 1;
			}
			FILE *outputImageFD = fopen( modifiedPictureName , "wt" );
			if( !outputImageFD ) {
				fclose( inputImageFD );
				freeNeighbours(neighbours,iterator);
				free(readBuffer);
	 			fclose(topFD);
			 	fclose(imageListFD);
			    MPI_Finalize();
			    fprintf(stderr,"I could not open the output image file !\n");
				return 1;
			}
			char ch, auxBuffer1[50];

            //Se incepe citirea imaginilor.
			fscanf(inputImageFD,"P%c\n",&ch);
			fgets(auxBuffer1,50,inputImageFD);
			int numberOfLines, numberOfColumns, maxVal;
			fscanf(inputImageFD,"%d %d\n%d", &numberOfColumns, &numberOfLines,
				&maxVal);

            //Se determina filtrul ce trebuie aplicat.
			const int flag = (!strncmp("smooth",readBuffer,6)) ? 1 :
				(!strncmp("blur",readBuffer,4)) ? 2 :
				(!strncmp("sharpen",readBuffer,4)) ? 3 :
				(!strncmp("mean_removal",readBuffer,4)) ? 4 : 5;

			numberOfColumns += 2;
			numberOfLines += 2;

			int *pictureBuffer = (int*)malloc( numberOfLines * numberOfColumns * sizeof(int) );
			if( !pictureBuffer ){
				fclose(outputImageFD);
				fclose( inputImageFD );
				freeNeighbours(neighbours,iterator);
				free(readBuffer);
	 			fclose(topFD);
			 	fclose(imageListFD);
			    MPI_Finalize();
			    fprintf(stderr,"I could not allocate memory !\n");
				return 1;
			}

			//Se creeaza padding-ul care va folosi la aplicarea filtrului.
			int b, c;
			for( c = 0 ; c < numberOfColumns ; c++ )
				pictureBuffer[c] = 0;
			numberOfLines--;
			char ch1;
			for( b = 1 ; b < numberOfLines ; b++ ){
				pictureBuffer[b * numberOfColumns] = 0;
				for( c = 1 ; c < numberOfColumns - 1 ; c++ )
					fscanf(inputImageFD,"%d",&(pictureBuffer[b*numberOfColumns+c]));
				pictureBuffer[b * numberOfColumns + numberOfColumns - 1] = 0;
			}
			int d = numberOfLines * numberOfColumns;
			for( c = 0 ; c < numberOfColumns ; c++ )
				pictureBuffer[d + c] = 0;
			numberOfLines--;

			//Se determina numarul de linii de procesat sau dat mai departe pentru fiecare copil.
			int linesToProcessForChild[numberOfChildren];
			for( d = 0 ; d < numberOfChildren; d++ )
					linesToProcessForChild[d] = 0;
			c = numberOfLines;
			while( c > 0 )
				for( d = 0 ; d < numberOfChildren && c > 0 ; d++ ){
					linesToProcessForChild[d]++;
					c--;
				}

			b = 0;
			c = 0;
			iterator = neighbours;
			//Se incepe trimiterea de informatii catre copii.
			while( iterator ){
				d = (linesToProcessForChild[c]+2)*numberOfColumns;
				MPI_Send(&d,1,MPI_INT, iterator->info,flag,MPI_COMM_WORLD);
				MPI_Send(&numberOfColumns,1,MPI_INT, iterator->info,flag,MPI_COMM_WORLD);
				MPI_Send(&(pictureBuffer[b*numberOfColumns]),d,MPI_INT,iterator->info,flag,MPI_COMM_WORLD);
				iterator = iterator->next;
				b += linesToProcessForChild[c];
				c++;
			}

            //Se centralizeaza datele prelucrate de la copii.
			numberOfColumns -= 2;
			fprintf(outputImageFD, "P%c", ch);
			fprintf(outputImageFD, "\n%s", auxBuffer1);
			fprintf(outputImageFD, "%d %d\n%d", numberOfColumns, numberOfLines , maxVal);
			c = 0;
			for( iterator = neighbours ; iterator ; iterator = iterator->next ){
				d = linesToProcessForChild[c] * numberOfColumns;
				MPI_Recv(pictureBuffer,d,MPI_INT,
					iterator->info,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);
				for( b = 0 ; b < d ; b++ )
					fprintf(outputImageFD, "\n%d", pictureBuffer[b]);
				c++;
			}

            //Se trateaza cazul in care exista un singur nod in graf.
            if( !numberOfChildren ){

                signed char *filter;
					switch( flag ){
						case 1:
							filter = smoothMatrix;
							break;
						case 2:
							filter = blurMatrix;
							break;
						case 3:
							filter = sharpenMatrix;
							break;
						case 4:
							filter = meanRemovalMatrix;
							break;
					}

                //Se aplica filtrul.
                int j,mPictureSectionBuffer;
					for( i = 1 ; i < numberOfLines - 1 ; i++ )
						for( j = 1 ; j < numberOfColumns - 1 ; j++ ){
							int sum = 0, b, c, d = 0;
							for( b = i - 1 ; b <= i + 1 ; b++ )
								for( c = j - 1 ; c <= j + 1 ; c++ ){
									sum = sum + pictureBuffer[b * numberOfColumns + c] * filter[d];
									d++;
								}
							mPictureSectionBuffer = sum / filter[d];
							if( mPictureSectionBuffer > 255 )
								mPictureSectionBuffer = 255;
							if( mPictureSectionBuffer < 0 )
								mPictureSectionBuffer = 0;
                            fprintf(outputImageFD, "\n%d", mPictureSectionBuffer);
						}
            }

			free( pictureBuffer );
			fclose( inputImageFD );
			fclose( outputImageFD );
		}

        //Se trimite semnalul de incheiere a procesarii care are tag-ul 5;
		int countBuffer[n], numberOfLinesProcBy[n];
		for( j = 0 ; j < n ; j++ )
			numberOfLinesProcBy[j] = 0;
		for( iterator = neighbours ; iterator ; iterator = iterator->next )
			MPI_Send(&i,1,MPI_INT,iterator->info,5,MPI_COMM_WORLD);

		//Se centralizeaza datele legate de statistica.
		for( iterator = neighbours ; iterator ; iterator = iterator->next ){
			MPI_Recv(countBuffer,n,MPI_INT,iterator->info,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);
			for( j = 0 ; j < n ; j++ )
				if( countBuffer[j] != 0 )
					numberOfLinesProcBy[j] = countBuffer[j];
		}
		FILE *statisticFD = fopen( argv[3] , "wt" );
		for(j=0;j<n;j++)
			fprintf(statisticFD, "%d: %d\n", j , numberOfLinesProcBy[j]);
		fclose(statisticFD);
	} else {

        //Se primeste sondaj de la parinte si se trimite ecoul inapoi.
		int parent;
		MPI_Recv(&i,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&recvStatus);
		parent = recvStatus.MPI_SOURCE;
		i = 1;
		MPI_Send(&i,1,MPI_INT, parent,0,MPI_COMM_WORLD);

        //Se trimite semnalul de sondaj mai departe nodurilor-vecini inafar
        //de nodul-parinte.
		iterator = neighbours;
		i = 0;
		while(iterator){
			if( iterator->info != parent )
				MPI_Send(&i,1,MPI_INT, iterator->info,0,MPI_COMM_WORLD);
			iterator = iterator->next;
		}

		newCell = NULL;
		iterator = neighbours;
		//Se sterg nodurile-vecin in functie de cum raspund la sondaj.
		while(iterator)
			if( iterator->info != parent ){
				MPI_Recv(&i,1,MPI_INT,iterator->info,0,MPI_COMM_WORLD,&recvStatus);
				if( i == 0 ){
					if( newCell == NULL ){
						newCell = iterator;
						iterator = iterator->next;
						neighbours = iterator;
						free(newCell);
						newCell = NULL;
					} else {
						newCell->next = iterator->next;
						struct simpleLinkedList *aux = iterator;
						iterator = iterator->next;
						free(aux);
					}
				} else {
					newCell = iterator;
					iterator = iterator->next;
				}
			} else {
                if( newCell == NULL ){
                    newCell = iterator;
                    iterator = iterator->next;
                    neighbours = iterator;
                    free(newCell);
                    newCell = NULL;
                } else {
                    newCell->next = iterator->next;
                    struct simpleLinkedList *aux = iterator;
                    iterator = iterator->next;
                    free(aux);
                }
			}

        //Se determina numarul de copii.
		int numberOfChildren = 0;
		for( iterator = neighbours ; iterator ; iterator = iterator->next )
			numberOfChildren++;

		int numberOfProcLines = 0;

		while(1){
            //Se primeste un semnal pe baga caruia se decide ce trebuie sa faca nodul curent,
			MPI_Recv(&i,1,MPI_INT,parent,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);

			if( recvStatus.MPI_TAG != 5 ){
				int numberOfColumns;
				MPI_Recv(&numberOfColumns,1,MPI_INT,parent,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);
				int numberOfLines = i / numberOfColumns;

				int *pictureSectionBuffer = (int*)malloc(i*sizeof(int));
				if( !pictureSectionBuffer ){
					freeNeighbours(neighbours,iterator);
					free(readBuffer);
				 	fclose(topFD);
				 	fclose(imageListFD);
				    MPI_Finalize();
				    fprintf(stderr,"I could not allocate memory !\n");
				    return 1;
				}
				MPI_Recv(pictureSectionBuffer,i,MPI_INT,parent,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);

				int *mPictureSectionBuffer = (int*)malloc((numberOfLines-2)*(numberOfColumns-2)*sizeof(int));
				if( !mPictureSectionBuffer ){
					free(pictureSectionBuffer);
					freeNeighbours(neighbours,iterator);
					free(readBuffer);
				 	fclose(topFD);
				 	fclose(imageListFD);
				    MPI_Finalize();
				    fprintf(stderr,"I could not allocate memory !\n");
				    return 1;
				}


				if( !numberOfChildren ){
                    //Incepe codul asociat unu nod-frunza.
					numberOfProcLines += numberOfLines - 2;

					//Se selecteaza filtrul ce trebuie aplicat.
					signed char *filter;
					switch( recvStatus.MPI_TAG ){
						case 1:
							filter = smoothMatrix;
							break;
						case 2:
							filter = blurMatrix;
							break;
						case 3:
							filter = sharpenMatrix;
							break;
						case 4:
							filter = meanRemovalMatrix;
							break;
					}

					//Se aplica filtrul.
					int j, k = 0;
					for( i = 1 ; i < numberOfLines - 1 ; i++ )
						for( j = 1 ; j < numberOfColumns - 1 ; j++ ){
							int sum = 0, b, c, d = 0;
							for( b = i - 1 ; b <= i + 1 ; b++ )
								for( c = j - 1 ; c <= j + 1 ; c++ ){
									sum = sum + pictureSectionBuffer[b * numberOfColumns + c] * filter[d];
									d++;
								}
							mPictureSectionBuffer[k] = sum / filter[d];
							if( mPictureSectionBuffer[k] > 255 )
								mPictureSectionBuffer[k] = 255;
							if( mPictureSectionBuffer[k] < 0 )
								mPictureSectionBuffer[k] = 0;
							k++;
						}

				} else {
                    //Incepe codul asociat unu nod intermediar.

					//Se calculeaza numarul de linii de procesat sau dat mai departe pentru fiecare copil.
					int linesToProcessForChild[numberOfChildren], b, c, d;
					for( d = 0 ; d < numberOfChildren; d++ )
							linesToProcessForChild[d] = 0;
					c = numberOfLines - 2;
					while( c > 0 )
						for( d = 0 ; d < numberOfChildren && c > 0 ; d++ ){
							linesToProcessForChild[d]++;
							c--;
						}

					b = 0;
					c = 0;
					iterator = neighbours;
					//Se trimit datele la copii.
					while( iterator ){
						d = (linesToProcessForChild[c]+2)*numberOfColumns;
						MPI_Send(&d,1,MPI_INT, iterator->info,recvStatus.MPI_TAG,MPI_COMM_WORLD);
						MPI_Send(&numberOfColumns,1,MPI_INT, iterator->info,recvStatus.MPI_TAG,MPI_COMM_WORLD);
						MPI_Send(&(pictureSectionBuffer[b*numberOfColumns]),d,MPI_INT,iterator->info,recvStatus.MPI_TAG,MPI_COMM_WORLD);
						iterator = iterator->next;
						b += linesToProcessForChild[c];
						c++;
					}

					b = 0;
					c = 0;
					//Se centralizeaza datele de la copii.
					for( iterator = neighbours ; iterator ; iterator = iterator->next ){
						d = linesToProcessForChild[c] * (numberOfColumns - 2);
						MPI_Recv(&(mPictureSectionBuffer[b]), d ,MPI_INT,
							iterator->info,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);
						c++;
						b += d;
					}
				}

				//Se trimit datele centralizate la parinte.
				MPI_Send(mPictureSectionBuffer,(numberOfColumns-2)*(numberOfLines-2),MPI_INT,parent,0,MPI_COMM_WORLD);
				free(mPictureSectionBuffer);
				free(pictureSectionBuffer);
			} else{
                //Incepe bucata de cod ce descrie comportamentul nodului in cazul primirii semnalului de terminare
                //asociat tag-ului cu valoarea "5".
				for( iterator = neighbours ; iterator ; iterator = iterator->next )
					MPI_Send(&numberOfChildren,1,MPI_INT,iterator->info,5,MPI_COMM_WORLD);
				int i;
				int countBuffer[n], sendCountBuffer[n];
				for(  i = 0 ; i < n ; i++ )
					sendCountBuffer[i] = 0;
				sendCountBuffer[r] = numberOfProcLines;
				for( iterator = neighbours ; iterator ; iterator = iterator->next ){
					MPI_Recv(countBuffer,n,MPI_INT,iterator->info,MPI_ANY_TAG,MPI_COMM_WORLD,&recvStatus);
					int j;
					for( j = 0 ; j < n ; j++ )
						if( countBuffer[j] )
							sendCountBuffer[j] = countBuffer[j];
				}
				MPI_Send(sendCountBuffer,n,MPI_INT,parent,5,MPI_COMM_WORLD);
				break;
			}
		}
	}

    //Se elibereaza resurse.
	freeNeighbours(neighbours,iterator);
	free(readBuffer);
 	fclose(topFD);
 	fclose(imageListFD);
    MPI_Finalize();
	return 0;
}
