package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type Movie struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	ReleaseYear int16     `json:"release_year"`
	Poster      string    `json:"poster"`
	Director    *Director `json:"director"`
}

type Director struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

var db *sql.DB

// Connect to the PostgreSQL database
func connectToDB() {
	var err error
	connStr := "user=rajneeshverma dbname=movies_db sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Could not connect to the database: ", err)
	}
}

// Get all movies
func getMovies(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	rows, err := db.Query("SELECT id, title, description, release_year, poster, director_first_name, director_last_name FROM movies")
	if err != nil {
		http.Error(w, "Unable to fetch movies", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var movies []Movie
	for rows.Next() {
		var movie Movie
		var directorFirstName, directorLastName string
		if err := rows.Scan(&movie.ID, &movie.Title, &movie.Description, &movie.ReleaseYear, &movie.Poster, &directorFirstName, &directorLastName); err != nil {
			http.Error(w, "Error scanning movie", http.StatusInternalServerError)
			return
		}
		movie.Director = &Director{FirstName: directorFirstName, LastName: directorLastName}
		movies = append(movies, movie)
	}

	json.NewEncoder(w).Encode(movies)
}

func getMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)

	row := db.QueryRow("SELECT id, title, description, release_year, poster, director_first_name, director_last_name FROM movies WHERE id=$1", params["id"])

	var movie Movie
	var directorFirstName, directorLastName string
	if err := row.Scan(&movie.ID, &movie.Title, &movie.Description, &movie.ReleaseYear, &movie.Poster, &directorFirstName, &directorLastName); err != nil {
		http.Error(w, "Movie not found", http.StatusNotFound)
		return
	}
	movie.Director = &Director{FirstName: directorFirstName, LastName: directorLastName}
	json.NewEncoder(w).Encode(movie)
}

// Create a new movie
func createMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var movie Movie

	if err := json.NewDecoder(r.Body).Decode(&movie); err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// Insert into the database
	var id string
	err := db.QueryRow(
		"INSERT INTO movies (title, description, release_year, poster, director_first_name, director_last_name) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
		movie.Title, movie.Description, movie.ReleaseYear, movie.Poster, movie.Director.FirstName, movie.Director.LastName).Scan(&id)
	if err != nil {
		http.Error(w, "Failed to create movie", http.StatusInternalServerError)
		return
	}
	movie.ID = id

	json.NewEncoder(w).Encode(movie)
}

// Delete a movie
func deleteMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)

	_, err := db.Exec("DELETE FROM movies WHERE id=$1", params["id"])
	if err != nil {
		http.Error(w, "Failed to delete movie", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"message": "Movie deleted"})
}

// Update a movie
func updateMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	var movie Movie

	if err := json.NewDecoder(r.Body).Decode(&movie); err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// Update the movie in the database
	_, err := db.Exec(
		"UPDATE movies SET title=$1, description=$2, release_year=$3, poster=$4, director_first_name=$5, director_last_name=$6 WHERE id=$7",
		movie.Title, movie.Description, movie.ReleaseYear, movie.Poster, movie.Director.FirstName, movie.Director.LastName, params["id"])
	if err != nil {
		http.Error(w, "Failed to update movie", http.StatusInternalServerError)
		return
	}

	movie.ID = params["id"]
	json.NewEncoder(w).Encode(movie)
}

func main() {
	connectToDB()
	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/movies", getMovies).Methods("GET")
	r.HandleFunc("/movies/{id}", getMovie).Methods("GET")
	r.HandleFunc("/movies", createMovie).Methods("POST")
	r.HandleFunc("/movies/{id}", updateMovie).Methods("PUT")
	r.HandleFunc("/movies/{id}", deleteMovie).Methods("DELETE")

	// Start the server
	fmt.Println("Server is running on http://localhost:8080")
	http.ListenAndServe(":8080", r)
}
