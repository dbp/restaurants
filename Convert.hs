{-# LANGUAGE ScopedTypeVariables, OverloadedStrings #-}

import Data.Traversable (forM)
import Control.Applicative
import Control.Monad (void)

import qualified Data.Vector as V
import qualified Data.Csv.Streaming as CSV
import Data.Csv (FromField, FromRecord(..), Record(..), Parser, unsafeIndex)
import Database.PostgreSQL.Simple.ToField (ToField(..))
import Database.PostgreSQL.Simple.ToRow (ToRow(..))
import qualified Data.ByteString.Lazy as L
import Database.PostgreSQL.Simple
import Data.Text (Text)

-- NOTE(dbp 2014-06-15): need bigger tuples!
lengthMismatch :: Int -> Record -> Parser a
lengthMismatch expected v =
    fail $ "cannot unpack array of length " ++
    show n ++ " into a " ++ desired ++ ". Input record: " ++
    show v
  where
    n = V.length v
    desired | expected == 1 = "Only"
            | expected == 2 = "pair"
            | otherwise     = show expected ++ "-tuple"

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g, FromField h, FromField i, FromField j,
          FromField k, FromField l, FromField m, FromField n, FromField o) =>
         FromRecord (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) where
    parseRecord v
        | n == 15    = (,,,,,,,,,,,,,,) <$> unsafeIndex v 0
                                        <*> unsafeIndex v 1
                                        <*> unsafeIndex v 2
                                        <*> unsafeIndex v 3
                                        <*> unsafeIndex v 4
                                        <*> unsafeIndex v 5
                                        <*> unsafeIndex v 6
                                        <*> unsafeIndex v 7
                                        <*> unsafeIndex v 8
                                        <*> unsafeIndex v 9
                                        <*> unsafeIndex v 10
                                        <*> unsafeIndex v 11
                                        <*> unsafeIndex v 12
                                        <*> unsafeIndex v 13
                                        <*> unsafeIndex v 14
        | otherwise = lengthMismatch 15 v
          where
            n = V.length v


instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f,
          ToField g, ToField h, ToField i, ToField j, ToField k, ToField l,
          ToField m, ToField n, ToField o)
    => ToRow (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o) where
    toRow (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o) =
        [toField a, toField b, toField c, toField d, toField e, toField f,
         toField g, toField h, toField i, toField j, toField k, toField l,
         toField m, toField n, toField o]

main = do file <- L.readFile "WebExtract.txt"
          conn <- connectPostgreSQL "host='localhost' dbname='restaurants' user='restaurants' password='111'"
          execute_ conn "CREATE TABLE IF NOT EXISTS raw_data (id serial primary key, camis text,dba text,boro text,building text,street text,zipcode text,phone text,cuisinecode text,inspdate text,action text,violcode text,score text,currentgrade text,gradedate text,recorddate text);"
          execute_ conn "DELETE FROM raw_data;"
          process 0 (CSV.decode CSV.HasHeader file) $ \(camis :: Text,dba :: Text,boro :: Text,building :: Text,street :: Text,zipcode :: Text,phone :: Text,cuisinecode :: Text,inspdate :: Text,action :: Text,violcode :: Text,score :: Text,currentgrade :: Text,gradedate :: Text,recorddate :: Text) ->
           void $ execute conn "INSERT INTO raw_data (camis,dba,boro,building,street,zipcode,phone,cuisinecode,inspdate,action,violcode,score,currentgrade,gradedate,recorddate) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" (camis,dba,boro,building,street,zipcode,phone,cuisinecode,inspdate,action,violcode,score,currentgrade,gradedate,recorddate)
          return ()
  where process cnt (CSV.Nil Nothing _) f = return ()
        process cnt (CSV.Nil (Just err) _) f = putStrLn $ "ERROR " ++ (show cnt) ++ ": " ++ (show err)
        process cnt (CSV.Cons (Right row) xs) f = do f row
                                                     process (cnt+1) xs f
        process cnt (CSV.Cons (Left err) xs) f = do putStrLn $ "ERROR ON ROW " ++ (show cnt) ++ ": " ++ (show err)
                                                    process (cnt+1) xs f
